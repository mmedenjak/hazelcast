# Design document template

### Table of Contents

+ [Background](#background)
  - [Description](#description)
      * [Goals](#goals)
      * [Non-goals](#non-goals)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
      * [Java marker interface](#java-marker-interface)
      * [Declarative configuration](#declarative-configuration)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)

### Background
#### Description

When reading data from a distributed structure, one of the sources of GC pressure are "defensive copies" of the stored data. They are meant to provide isolation between the data stored and data which is provided to the user. This isolation shoud prevent issues that may arise when the user mutates the returned data, thus potentially mutating the stored data as well. This might also include concurrency issues if the returned object is not properly sychronized, something that should be implemented by the user and cannot be guaranteed by IMDG. On the other hand, defensive copies may incur a performance impact which is undesireable when the user knows the returned data cannot be mutated.

Defensive copies are important only for local data access (when using embedded IMDG members) and when the in-memory format is `OBJECT`.

##### Goals

First, we will investigate the impact of defensive copies on local reads. This mostly relates to GC pressure but may indirectly expose as latency spikes. It is very likely that the exact impact heavily depends on the complexity and size of the object - in some cases it might have a bigger performance impact and in other cases the performance impact might be insignificant when compared to other sources of latency:

> A lot of time is wasted on scheduling an operation on the partition thread and then block waiting for the result. So unless you have a very large object or the serialization is very slow, I doubt the amount of value you will gain from clone protection. So I would suggest doing a POC (just do some nasty hacking) and see how much performance improvement actually is possible. You still don't see what happens at the OS level and you can't see the time wasted to the context-switching/thread notification etc.

On the other hand, there are definite indications from customers that this feature might provide some benefit:

> HD didn’t help with this problem - they have immutable data and the aggregation logic was expressed as a Jet pipeline - and from what I understand not easy to convert to something like SQL - the user reported 6x increase in performance with avoiding deserialization. My understanding is that there are a series of use cases where it’s desirable to keep the object in a “ready to consume” form, and having some binary representation isn’t always the most performant for this. This is true for compute-heavy use cases and is indeed an area where we seem to have an edge vs Redis or Aerospike or things like Cassandra.
> > My benchmark is for running it on my own computer, using data that I’ve artificially increased from 25,000 real items to 400,000 items. I’ve compared against the lazy serialiser I wrote that deserialises fields on demand, which was efficient for the first stage of my process (if you remember) where I access 4 fields out of in this case 56, but I forced-fetched all of them because in the second stage of my process (which I haven’t written yet) I need access to almost all of them. In real life, if I’m forced to deserialise, maybe I’d do something different and more efficient. So really quite a lot of caveats, but given this, I got a mean of 10,262 ms using BINARY format and the lazy serialiser, and 1,657 ms using OBJECT format and no serialisation 

>  The reason why Adobe benefited from this feature so much is simple: They have complex objects with 56 fields and the Jet job was only interested in 4 fields. So 52 out of 56 fields were cloned only to be thrown away in the next phase. Obviously when you eliminate this waste then you save a lot of CPU cycles.

##### Non-goals  

Currently this optimisation will only apply:
 - when the reads are local (on IMDG members)
 - when the in-memory format is `OBJECT` and
 - only for `IMap` values. Keys are always stored in a serialized form, regardless of the `in-memory-format`. 
 
There are indications that there are places inside client code that may benefit from knowledge that an object is immutable (for instance, when reading from near-cache), but for now we focus our efforts on optimisations when reading data from embedded members.

Also, we limit ourselves to covering only some `IMap` API. Namely, these methods:
- map.get/getAll
- map.put/putAll
- MapPartitionIterator
- map.localKeySet()
- EntryProcessor

Ideally, an immutable object should never be defensively copied, but there is a lot of features and API to cover, and we ought to start simple and add optimisations as we see room for further improvement, rather than a big-bang approach. The good thing is that these optimisations are backwards compatible and can be introduced even in patch releases.  
 
### User Interaction
#### API design and/or Prototypes

The user will be able to denote that some objects are immutable in two ways.

##### Java marker interface

An example of this interface is listed below. This approach is most convenient for java users which have control over the classes being stored and it can provide fine granularity of control which classes of objects should be copied and which should not.  

```java
/**
 * Allows notifying Hazelcast code that the object implementing this interface is effectively immutable.
 * This may mean that it either does not have any state (e.g. pure function) or the state is
 * not mutated at any point.
 * This interface allows for performance optimisations where applicable such as avoiding
 * cloning user supplied objects or cloning hazelcast internal objects supplied to the user.
 * It is important that the user follows the rules:
 * <ul>
 * <li>the object must not have any state which is changed by cloning the object</li>
 * <li>the existing state must not be changed</li>
 * </ul>
 * If an object implements this interface but does not follow these rules, the results of the
 * execution are undefined.
 */
@Beta
public interface Immutable {
}
```

##### Declarative configuration

Some users might not have control over the objects being stored or they might not be storing java objects (e.g. JSON). In such cases, the declarative configuration should provide a more coarse-grained configuration. Examples of such configuration are listed below.

```xml
<map name="default">
  <immutable-values>true</immutable-values>
  ...
</map>
```

```yaml
  map:
    default:
      immutable-values: true
      ...
```  

### Technical Design

Once an object or map has been marked as immutable, we will have to perform one of these checks:
- check `instanceof` of stored value
- check map configuration to see if values are immutable. This check can be cached.

In addition to this, following classes need not be defensively copied by default - String, Integer and other basic Java types. Once this check passes, we may pass the reference to the value directly back to the user. In some cases, the return values of internal methods will have to be changed as they might assume the value will always be serialized before returning back to the user.

### Testing Criteria

First and foremost, it will be important to quantify how much of a performance impact this optimisation can bring. We assume the benefits grow with these two factors:
- the size of the stored object. Having larger objects may contribute to higher GC pressure, thus leading to more GC pauses
- the complexity of the serialisation/deserialization mechanism. Having a very poor ser-de mechanism may cause both CPU and GC pressure and in this case simply providing a reference to the stored object completely elides this entire overhead. In some cases the user might need only parts of the object, in which partial deserialisation may help, but it still isn't equal to simply passing a reference to the object itself, assuming that the user has enough memory to store all objects on-heap in a deserialized format.  

In addition to this, all existing `IMap` functionality should not have any regressions due to the use of these features. As such, we may parametrize some existing tests to run with and without immutable values. 
package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import jdk.internal.org.objectweb.asm.ClassWriter;
import jdk.internal.org.objectweb.asm.FieldVisitor;
import jdk.internal.org.objectweb.asm.MethodVisitor;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Character.toUpperCase;
import static jdk.internal.org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static jdk.internal.org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static jdk.internal.org.objectweb.asm.Opcodes.ACC_SUPER;
import static jdk.internal.org.objectweb.asm.Opcodes.ALOAD;
import static jdk.internal.org.objectweb.asm.Opcodes.GETFIELD;
import static jdk.internal.org.objectweb.asm.Opcodes.ILOAD;
import static jdk.internal.org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static jdk.internal.org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static jdk.internal.org.objectweb.asm.Opcodes.IRETURN;
import static jdk.internal.org.objectweb.asm.Opcodes.PUTFIELD;
import static jdk.internal.org.objectweb.asm.Opcodes.RETURN;
import static jdk.internal.org.objectweb.asm.Opcodes.V1_6;

public class PortableWrapperCompiler {
    private final AtomicInteger suffixId = new AtomicInteger(1);
    private final ChildClassLoader ccl;
    private final HashMap<ClassDefinition, Class> compiledMap = new HashMap<ClassDefinition, Class>();

    public PortableWrapperCompiler() {
        this.ccl = new ChildClassLoader(getDefaultClassLoader());
    }

    public Class compile(ClassDefinition cd, String name) {
        return getCompiledClass(cd, name);
    }

    private Class getCompiledClass(ClassDefinition cd, String name) {
        if (compiledMap.containsKey(cd)) {
            return compiledMap.get(cd);
        }
        synchronized (this) {
            if (compiledMap.containsKey(cd)) {
                return compiledMap.get(cd);
            }
            Class clazz = createClass(cd, name);
            if (clazz != null) {
                try {
                    compiledMap.put(cd, clazz);
                    return clazz;
                } catch (Throwable ex) {
                    throw new IllegalStateException("Failed to instantiate class", ex);
                }
            }
            return null;
        }
    }

    private int getNextSuffix() {
        return this.suffixId.incrementAndGet();
    }

    private String capitalize(String n) {
        return toUpperCase(n.charAt(0)) + n.substring(1);
    }

    private Class createClass(ClassDefinition classDef, String name) {
        final String className = "com/hazelcast/map/PortableWrapper" + getNextSuffix();
        final String superClass = name.replace(".", "/");

        final ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
        cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, className, null, superClass,
                new String[]{"com/hazelcast/map/impl/operation/PortableReaderWriterSetter"});

        createFieldAndConstructor(className, superClass, cw);
        final HashMap<FieldType, Types> typeMap = new HashMap<FieldType, Types>();

        typeMap.put(FieldType.BOOLEAN, Types.BOOLEAN);
        typeMap.put(FieldType.INT, Types.INT);
        typeMap.put(FieldType.LONG, Types.LONG);

        for (String fieldName : classDef.getFieldNames()) {
            final FieldDefinition fd = classDef.getField(fieldName);
            final FieldType type = fd.getType();
            final Types t = typeMap.get(type);
            if (t != null) {
                generateGetter(className, cw, fieldName, t);
                generateSetter(className, cw, fieldName, t);
            }
        }

        byte[] data = cw.toByteArray();
        return this.ccl.defineClass(className.replaceAll("/", "."), data);
    }

    private void generateSetter(String className, ClassWriter cw, String fieldName, Types type) {
        MethodVisitor mv;
        mv = cw.visitMethod(ACC_PUBLIC, "set" + capitalize(fieldName), type.setterReturnType, null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitFieldInsn(GETFIELD, className, "rw", "Lcom/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter;");
        mv.visitLdcInsn(fieldName);
        mv.visitVarInsn(ILOAD, 1);
        mv.visitMethodInsn(INVOKEVIRTUAL, "com/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter",
                type.portableWriteMethodName, type.portableWriteMethodSignature, false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(3, 2);
        mv.visitEnd();
    }

    private void generateGetter(String className, ClassWriter cw, String fieldName, Types type) {
        MethodVisitor mv;
        mv = cw.visitMethod(ACC_PUBLIC, "get" + capitalize(fieldName), type.getterReturnType, null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitFieldInsn(GETFIELD, className, "rw", "Lcom/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter;");
        mv.visitLdcInsn(fieldName);
        mv.visitMethodInsn(INVOKEVIRTUAL, "com/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter",
                type.portableReadMethodName, type.portableReadMethodSignature, false);
        mv.visitInsn(IRETURN);
        mv.visitMaxs(2, 1);
        mv.visitEnd();
    }

    private void createFieldAndConstructor(String className, String superClass, ClassWriter cw) {
        FieldVisitor fv;
        MethodVisitor mv;
        fv = cw.visitField(ACC_PRIVATE, "rw",
                "Lcom/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter;", null, null);
        fv.visitEnd();

        mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, superClass, "<init>", "()V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();

        mv = cw.visitMethod(ACC_PUBLIC, "setPortableReaderWriter",
                "(Lcom/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter;)V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitFieldInsn(PUTFIELD, className, "rw", "Lcom/hazelcast/internal/serialization/impl/DefaultPortableReaderWriter;");
        mv.visitInsn(RETURN);
        mv.visitMaxs(2, 2);
        mv.visitEnd();
    }


    private static class ChildClassLoader extends URLClassLoader {

        private static final URL[] NO_URLS = new URL[0];

        public ChildClassLoader(ClassLoader classLoader) {
            super(NO_URLS, classLoader);
        }

        public Class<?> defineClass(String name, byte[] bytes) {
            return super.defineClass(name, bytes, 0, bytes.length);
        }

    }

    public enum Types {
        INT("()I", "readInt", "(Ljava/lang/String;)I", "(I)V", "writeInt", "(Ljava/lang/String;I)V"),
        BOOLEAN("()Z", "readBoolean", "(Ljava/lang/String;)Z", "(Z)V", "writeBoolean", "(Ljava/lang/String;Z)V"),
        LONG("()J", "readLong", "(Ljava/lang/String;)J", "(J)V", "writeLong", "(Ljava/lang/String;J)V");

        private final String
                getterReturnType, portableReadMethodName, portableReadMethodSignature,
                setterReturnType, portableWriteMethodName, portableWriteMethodSignature;

        Types(String getterReturnType, String portableReadMethodName, String portableReadMethodSignature,
              String setterReturnType, String portableWriteMethodName, String portableWriteMethodSignature) {
            this.getterReturnType = getterReturnType;
            this.portableReadMethodName = portableReadMethodName;
            this.portableReadMethodSignature = portableReadMethodSignature;
            this.setterReturnType = setterReturnType;
            this.portableWriteMethodName = portableWriteMethodName;
            this.portableWriteMethodSignature = portableWriteMethodSignature;
        }
    }


    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = PortableWrapperCompiler.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }
}

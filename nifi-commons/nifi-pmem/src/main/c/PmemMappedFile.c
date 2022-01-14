/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <jni.h>

#include <errno.h>
#include <inttypes.h> // PRIxPTR
#include <limits.h> // PATH_MAX
#include <stdint.h> // uintptr_t
#include <stdio.h> // snprintf
#include <stdlib.h> // abort
#include <string.h> // strerror_r

#include <libpmem.h>

/*
 * PmemMappedFile(String, long, long, boolean); // (Ljava/lang/String;JJZ)V
 */
static jmethodID ctor;

/*
 * class java.nio.DirectByteBuffer;
 * DirectByteBuffer(long, int, Object); // (JILjava/lang/Object;)V
 *
 * We need this, not the JNI function NewDirectByteBuffer, because we should
 * keep a strong reference to a PmemMappedFile instance while any direct
 * ByteBuffer referring to it remains; or the PmemMappedFile instance will get
 * garbage-collected, leading to unexpected error.
 *
 * At least, OpenJDK 8u and 11u have such a constructor. See:
 * - https://github.com/AdoptOpenJDK/openjdk-jdk8u/blob/master/jdk/src/share/classes/java/nio/Direct-X-Buffer.java.template
 * - https://github.com/openjdk/jdk11u/blob/master/src/java.base/share/classes/java/nio/Direct-X-Buffer.java.template
 */
static jclass DirectByteBuffer;
static jmethodID view;

/*
 * class java.io.IOException;
 * class java.nio.file.AccessDeniedException;
 * class java.nio.file.FileAlreadyExistsException;
 * class java.nio.file.NoSuchFileException;
 */
static jclass IOException;
static jclass AccessDeniedException;
static jclass FileAlreadyExistsException;
static jclass NoSuchFileException;

/*
 * The ret_ can be empty; it should be empty if in a void function.
 * Note that the ret is not parenthesized.
 */
#define RETURN_IF_THROWN_ELSE_FATAL(env_, ret_, msg_)       \
    do {                                                    \
        JNIEnv *const scoped_env_ = (env_);                 \
        if ((*scoped_env_)->ExceptionCheck(scoped_env_)) {  \
            return ret_;                                    \
        }                                                   \
        (*scoped_env_)->FatalError(scoped_env_, (msg_));    \
        abort();                                            \
    } while (0)

static inline jlong p2j(void *p) {
    return (jlong) (uintptr_t) p;
}

static inline void *j2p(jlong j) {
    return (void *) (uintptr_t) j;
}

static inline jclass ioe_class_from_errno(int errnum) {
    switch (errnum) {
    case EACCES:
        return AccessDeniedException;
    case EEXIST:
        return FileAlreadyExistsException;
    case ENOENT:
        return NoSuchFileException;
    }
    return IOException;
}

static void ThrowNewIOException(JNIEnv *env, int errnum, const char *desc) {
    char errstr[256] = {0}; // probably enough length
    strerror_r(errnum, errstr, sizeof(errstr));

    char msg[PATH_MAX] = {0}; // desc may hold a path
    snprintf(msg, sizeof(msg), "[Errno %d] %s: %s", errnum, errstr, desc);

    (*env)->ThrowNew(env, ioe_class_from_errno(errnum), msg);
}

static void ThrowNewIOExceptionAddressLength(
        JNIEnv *env, int errnum, jlong address, jlong length) {
    /*
     * We need the following 55 chars on a 64-bit platform:
     *
     * 16 chars -- %# PRIxPTR (including "0x" prefix)
     * 20 chars -- %zd (including "-" sign)
     * 18 chars -- "address 0x length "
     *  1 char  -- NUL
     */
    char desc[64]; // enough length
    snprintf(desc, sizeof(desc), "address %#" PRIxPTR " length %zd",
            (uintptr_t) address, (ssize_t) length);

    ThrowNewIOException(env, errnum, desc);
}


static inline jclass NewGlobalClassRef(JNIEnv *env, const char *name) {
    jclass local_ref = (*env)->FindClass(env, name);
    return (local_ref == NULL) ? NULL : (*env)->NewGlobalRef(env, local_ref);
}

static inline jmethodID GetCtorID(JNIEnv *env, jclass clazz, const char *sig) {
    return (*env)->GetMethodID(env, clazz, "<init>", sig);
}

JNIEXPORT void JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_init(JNIEnv *env, jclass clazz) {
    ctor = GetCtorID(env, clazz, "(Ljava/lang/String;JJZ)V");
    if (ctor == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */, "GetCtorID(PmemMappedFile) in init");
    }

    DirectByteBuffer = NewGlobalClassRef(env, "java/nio/DirectByteBuffer");
    if (DirectByteBuffer == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */, "NewGlobalClassRef(DirectByteBuffer) in init");
    }

    view = GetCtorID(env, DirectByteBuffer, "(JILjava/lang/Object;)V");
    if (view == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */, "GetCtorID(DirectByteBuffer) in init");
    }

    IOException = NewGlobalClassRef(env, "java/io/IOException");
    if (IOException == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */, "NewGlobalClassRef(IOException) in init");
    }

    AccessDeniedException = NewGlobalClassRef(
            env, "java/nio/file/AccessDeniedException");
    if (AccessDeniedException == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */,
                "NewGlobalClassRef(AccessDeniedException) in init");
    }

    FileAlreadyExistsException = NewGlobalClassRef(
            env, "java/nio/file/FileAlreadyExistsException");
    if (FileAlreadyExistsException == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */,
                "NewGlobalClassRef(FileAlreadyExistsException) in init");
    }

    NoSuchFileException = NewGlobalClassRef(
            env, "java/nio/file/NoSuchFileException");
    if (NoSuchFileException == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, /* void */,
                "NewGlobalClassRef(NoSuchFileException) in init");
    }
}

static jobject pmem_map_file_generic(
        JNIEnv *env, jclass clazz,
        jstring path, jlong length, int flags, jint mode) {
    const char *const c_path = (*env)->GetStringUTFChars(env, path, NULL);
    if (c_path == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, NULL, "GetStringUTFChars in pmem_map_file_generic");
    }

    size_t mapped_length = 0;
    int is_pmem = 0;
    void *const address = pmem_map_file(
            c_path, length, flags, mode, &mapped_length, &is_pmem);
    const int errnum = errno;

    jobject ret = NULL;
    if (address == NULL) {
        char desc[PATH_MAX] = {0};
        snprintf(desc, sizeof(desc),
                "path \"%s\" length %zd flags %d mode %#o",
                c_path, (ssize_t) length, flags, (int) mode);

        ThrowNewIOException(env, errnum, desc);
    } else {
        ret = (*env)->NewObject(env, clazz, ctor,
                path, p2j(address), mapped_length, is_pmem);
    }

    (*env)->ReleaseStringUTFChars(env, path, c_path);
    return ret;
}

JNIEXPORT jobject JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1map_1file_1open(
        JNIEnv *env, jclass clazz, jstring path) {
    return pmem_map_file_generic(env, clazz, path, 0, 0, 0);
}

JNIEXPORT jobject JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1map_1file_1create_1excl(
        JNIEnv *env, jclass clazz, jstring path, jlong length, jint mode) {
    static const int flags = PMEM_FILE_CREATE | PMEM_FILE_EXCL;
    return pmem_map_file_generic(env, clazz, path, length, flags, mode);
}

JNIEXPORT jobject JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1map_1file_1create(
        JNIEnv *env, jclass clazz, jstring path, jlong length, jint mode) {
    static const int flags = PMEM_FILE_CREATE;
    return pmem_map_file_generic(env, clazz, path, length, flags, mode);
}

JNIEXPORT void JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1unmap(
        JNIEnv *env, jclass clazz, jlong address, jlong length) {
    if (pmem_unmap(j2p(address), length) < 0) {
        ThrowNewIOExceptionAddressLength(env, errno, address, length);
    }
}

static jboolean pmem_memcpy_generic(
        JNIEnv *env, jclass clazz,
        jlong address, jbyteArray src, jint index, jint length, int flags) {
    jboolean is_copy = JNI_FALSE;
    jbyte *const c_src = (*env)->GetPrimitiveArrayCritical(env, src, &is_copy);
    if (c_src == NULL) {
        RETURN_IF_THROWN_ELSE_FATAL(
                env, JNI_FALSE,
                "GetPrimitiveArrayCritical in pmem_memcpy_generic");
    }

    pmem_memcpy(j2p(address), &c_src[index], length, flags);

    /*
     * We can use JNI_ABORT instead of 0 because the src is input-only;
     * we do not need write-back the c_src to the src.
     */
    (*env)->ReleasePrimitiveArrayCritical(env, src, c_src, JNI_ABORT);
    return is_copy;
}

JNIEXPORT jboolean JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1memcpy_1nodrain(
        JNIEnv *env, jclass clazz,
        jlong address, jbyteArray src, jint index, jint length) {
    static const int flags = PMEM_F_MEM_NODRAIN;
    return pmem_memcpy_generic(env, clazz, address, src, index, length, flags);
}

JNIEXPORT jboolean JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1memcpy_1noflush(
        JNIEnv *env, jclass clazz,
        jlong address, jbyteArray src, jint index, jint length) {
    static const int flags = PMEM_F_MEM_NOFLUSH;
    return pmem_memcpy_generic(env, clazz, address, src, index, length, flags);
}

JNIEXPORT void JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1flush(
        JNIEnv *env, jclass clazz, jlong address, jlong length) {
    pmem_flush(j2p(address), length);
}

JNIEXPORT void JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1drain(
        JNIEnv *env, jclass clazz) {
    pmem_drain();
}

JNIEXPORT void JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_pmem_1msync(
        JNIEnv *env, jclass clazz, jlong address, jlong length) {
    if (pmem_msync(j2p(address), length) < 0) {
        ThrowNewIOExceptionAddressLength(env, errno, address, length);
    }
}

JNIEXPORT jobject JNICALL
Java_org_apache_nifi_pmem_PmemMappedFile_newDirectByteBufferView(
        JNIEnv *env, jobject object, jlong address, jint length) {
    return (*env)->NewObject(
            env, DirectByteBuffer, view, address, length, object);
}

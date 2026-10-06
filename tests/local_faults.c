/* macOS-only test interposer. Never linked into the engine or shipped SDK.
 * Arm by creating DEOOS_FAULT_ARM after server startup. Rename modes consume it
 * for one owned object-directory prefix; fail-read-sync consumes it for one objects-directory
 * barrier in an otherwise idle reader. Startup barriers are untouched.
 */
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <unistd.h>

static const char *target, *arm, *marker, *mode;
static _Thread_local int fail_next_directory_sync;

__attribute__((constructor)) static void configure(void) {
    target = getenv("DEOOS_FAULT_TARGET");
    arm = getenv("DEOOS_FAULT_ARM");
    marker = getenv("DEOOS_FAULT_MARKER");
    mode = getenv("DEOOS_FAULT_MODE");
}

static void mark(const char *event) {
    int fd = open(marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) _exit(91);
    size_t len = strlen(event);
    if (write(fd, event, len) != (ssize_t)len || close(fd) != 0) _exit(92);
}

static int injected_rename(const char *from, const char *to) {
    int matched = target && arm && marker && mode && strncmp(to, target, strlen(target)) == 0
        && unlink(arm) == 0;
    if (matched && strcmp(mode, "before-rename") == 0) {
        mark("before-rename");
        raise(SIGSTOP);
    }
    /* dyld excludes this image when binding our direct original calls.
     * RTLD_NEXT dlsym is unsuitable here: dyld may return the interposer. */
    int result = rename(from, to);
    if (matched && result == 0) {
        if (strcmp(mode, "after-rename") == 0) {
            mark("after-rename");
            raise(SIGSTOP);
        } else if (strcmp(mode, "fail-sync") == 0) {
            fail_next_directory_sync = 1;
        }
    }
    return result;
}

static int injected_fsync(int fd) {
    /* A read never renames the target. This separate one-shot mode faults the
     * objects-directory barrier only after the test explicitly arms an idle
     * reader process. F_GETPATH scopes it to the owned target's directory. */
    if (target && arm && marker && mode && strcmp(mode, "fail-read-sync") == 0) {
        struct stat info;
        char path[MAXPATHLEN];
        const char *slash = strrchr(target, '/');
        if (slash && fstat(fd, &info) == 0 && S_ISDIR(info.st_mode)
                && fcntl(fd, F_GETPATH, path) == 0) {
            size_t length = (size_t)(slash - target);
            if (strlen(path) == length && strncmp(path, target, length) == 0
                    && unlink(arm) == 0) {
                mark("read-directory-fsync-EIO");
                errno = EIO;
                return -1;
            }
        }
    }
    if (fail_next_directory_sync) {
        struct stat info;
        if (fstat(fd, &info) == 0 && S_ISDIR(info.st_mode)) {
            fail_next_directory_sync = 0;
            mark("after-rename-directory-fsync-EIO");
            errno = EIO;
            return -1;
        }
    }
    return fsync(fd);
}

#define INTERPOSE(replacement, original) \
    __attribute__((used)) static struct { const void *replacement; const void *original; } \
    interpose_##original __attribute__((section("__DATA,__interpose"))) = \
    { (const void *)&replacement, (const void *)&original }
INTERPOSE(injected_rename, rename);
INTERPOSE(injected_fsync, fsync);

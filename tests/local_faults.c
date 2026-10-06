/* macOS-only test interposer. Never linked into the engine or shipped SDK.
 * Arm by creating DEOOS_FAULT_ARM after server startup. One exact destination
 * rename consumes that file; unrelated writes and startup barriers are untouched.
 */
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    int matched = target && arm && marker && mode && strcmp(to, target) == 0
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

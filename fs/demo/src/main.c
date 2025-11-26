/* main.c源码 */
#define _XOPEN_SOURCE 700

#define FUSE_USE_VERSION 26
#include "stdio.h"
#include "fuse.h"
#include "../include/ddriver.h"
#include <linux/fs.h>
#include "pwd.h"
#include "unistd.h"
#include "string.h"
#include <sys/types.h>
#include <sys/stat.h>

#define DEMO_DEFAULT_PERM        0777


/* 超级块 */
struct demo_super
{
    int     driver_fd;  /* 模拟磁盘的fd */

    int     sz_io;      /* 磁盘IO大小，单位B */
    int     sz_disk;    /* 磁盘容量大小，单位B */
    int     sz_blks;    /* 逻辑块大小，单位B（= 2 * sz_io） */
};

/* 目录项 */
struct demo_dentry
{
    char    fname[128];
}; 

/* 全局超级块 */
static struct demo_super super;

#define DEVICE_NAME "ddriver"

/* 挂载文件系统：打开模拟磁盘 + 填充 super 信息 */
static void* demo_mount(struct fuse_conn_info * conn_info)
{
    /* 1. 打开驱动文件（在当前用户 home 目录下的 ddriver） */
    char device_path[128] = {0};
    sprintf(device_path, "%s/" DEVICE_NAME, getpwuid(getuid())->pw_dir);
    super.driver_fd = ddriver_open(device_path);

    printf("super.driver_fd: %d\n", super.driver_fd);

    /* 2. 通过 ioctl 获取磁盘参数，填充 super 信息
     *
     * ddriver_ioctl 的函数声明为：
     *   int ddriver_ioctl(int fd, unsigned long cmd, void *ret);
     *
     * ddriver_ctl_user.h 中提供的命令：
     *   IOC_REQ_DEVICE_SIZE   —— 设备容量大小（字节）
     *   IOC_REQ_DEVICE_IO_SZ  —— 设备 IO 块大小（字节）
     */
    memset(&super.sz_io,   0, sizeof(super.sz_io));
    memset(&super.sz_disk, 0, sizeof(super.sz_disk));

    /* 获取 IO 块大小（单位：字节） */
    ddriver_ioctl(super.driver_fd, IOC_REQ_DEVICE_IO_SZ, &super.sz_io);

    /* 获取磁盘总大小（单位：字节） */
    ddriver_ioctl(super.driver_fd, IOC_REQ_DEVICE_SIZE, &super.sz_disk);

    /* 逻辑块大小 = 两个 IO 块大小（题目要求） */
    super.sz_blks = super.sz_io * 2;

    printf("sz_io   = %d bytes\n", super.sz_io);
    printf("sz_disk = %d bytes\n", super.sz_disk);
    printf("sz_blks = %d bytes (logic block size)\n", super.sz_blks);

    return NULL;
}

/* 卸载文件系统：关闭模拟磁盘 */
static void demo_umount(void* p)
{
    ddriver_close(super.driver_fd);
}

/* 遍历目录
 * 任务要求：从第 500 个逻辑块读取一个 demo_dentry，把其中的文件名填充到 filename，
 * 然后通过 filler 返回这个文件名（ls 只会看到这一个文件名）。
 */
static int demo_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                        off_t offset, struct fuse_file_info* fi)
{
    // 任务一这里不需要使用 path / offset / fi 等参数
    (void)path;
    (void)offset;
    (void)fi;

    char filename[128]; // 待填充的文件名
    memset(filename, 0, sizeof(filename));

    /* 根据超级块的信息，从第 500 逻辑块读取一个 dentry */

    /* 1. 计算磁盘偏移：第 500 个逻辑块的起始字节位置 */
    off_t off = (off_t)500 * super.sz_blks;

    /* 2. 移动磁盘头到该偏移位置 */
    ddriver_seek(super.driver_fd, off, SEEK_SET);

    /* 3. 读出一个磁盘块到内存
     *    题目明确说“读出一个磁盘块，512B”。
     *    如果你们的逻辑块大小本身就是 512B，可以直接用 512。
     *    如果想更通用，用 super.sz_blks 也可以。
     */
    char blkbuf[512];
    ddriver_read(super.driver_fd, blkbuf, sizeof(blkbuf));
    // 或者：
    // char *blkbuf = malloc(super.sz_blks);
    // ddriver_read(super.driver_fd, blkbuf, super.sz_blks);

    /* 4. 构造一个 demo_dentry，把前 sizeof(struct demo_dentry) 字节拷进去 */
    struct demo_dentry dentry;
    memcpy(&dentry, blkbuf, sizeof(struct demo_dentry));

    /* 5. 根据 dentry 的文件名填充 filename */
    strncpy(filename, dentry.fname, sizeof(filename) - 1);
    filename[sizeof(filename) - 1] = '\0';

    /* 这里不需要自己管 "." 和 ".."，老师给的框架里只要求你先能读出这个文件名。
     * filler 已经帮你封装好，只要 filename 正确，就能在 ls 下看到这个文件。
     */
    return filler(buf, filename, NULL, 0);
}

/* 显示文件属性
 * 要求：把该文件显示为“普通文件”。
 * - 根路径 "/"：显示为目录 (S_IFDIR)
 * - 其它路径：显示为普通文件 (S_IFREG)
 */
static int demo_getattr(const char* path, struct stat *stbuf)
{
    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) {
        /* 根目录属性：目录类型 + 默认权限 */
        stbuf->st_mode = DEMO_DEFAULT_PERM | S_IFDIR;
        stbuf->st_nlink = 2;  // "." 和 ".."
    } else {
        /* 其它路径：显示为普通文件 */
        stbuf->st_mode = DEMO_DEFAULT_PERM | S_IFREG;
        stbuf->st_nlink = 1;
        // stbuf->st_size 也可以随便给个值，这里先不要求
    }

    return 0;
}

/* 根据任务1需求 只用实现前四个钩子函数即可完成 ls 操作 */
static struct fuse_operations ops = {
    .init    = demo_mount,     /* mount 文件系统 */		
    .destroy = demo_umount,    /* umount 文件系统 */
    .getattr = demo_getattr,   /* 获取文件属性 */
    .readdir = demo_readdir,   /* 填充 dentrys */
};

int main(int argc, char *argv[])
{
    int ret = 0;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    ret = fuse_main(args.argc, args.argv, &ops, NULL);
    fuse_opt_free_args(&args);

    return ret;
}

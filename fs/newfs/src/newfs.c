#define _XOPEN_SOURCE 700

#include "newfs.h"
#include <stdbool.h>
#include <sys/stat.h>
#include <time.h>
#include <limits.h>

/******************************************************************************
* SECTION: 宏定义
*******************************************************************************/
#define OPTION(t, p)        { t, offsetof(struct custom_options, p), 1 }

#define NEWFS_BLOCK_SIZE    1024
#define NEWFS_DIRECT_NUM    8
#define BITS_PER_BYTE       8

static inline off_t round_down(off_t value, uint32_t align) {
        return (value / (off_t)align) * (off_t)align;
}

static inline off_t round_up(off_t value, uint32_t align) {
        return ((value + (off_t)align - 1) / (off_t)align) * (off_t)align;
}

/******************************************************************************
* SECTION: 结构体定义
*******************************************************************************/
struct newfs_super_d {
    uint32_t magic;
    uint32_t block_size;

    uint32_t sb_offset;
    uint32_t sb_blks;

    uint32_t ino_map_offset;
    uint32_t ino_map_blks;

    uint32_t data_map_offset;
    uint32_t data_map_blks;

    uint32_t inode_offset;
    uint32_t inode_blks;

    uint32_t data_offset;
    uint32_t data_blks;

    uint32_t inode_count;
    uint32_t data_count;
    uint32_t root_ino;
};

struct newfs_inode_d {
    uint32_t mode;
    uint32_t size;
    uint32_t links;
    uint32_t blocks[NEWFS_DIRECT_NUM];
};

struct newfs_dentry_d {
    char     name[MAX_NAME_LEN];
    uint32_t ino;
    uint32_t mode;
};

/******************************************************************************
* SECTION: 全局变量
*******************************************************************************/
static const struct fuse_opt option_spec[] = {          /* 用于FUSE文件系统解析参数 */
        OPTION("--device=%s", device),
        FUSE_OPT_END
};

struct custom_options newfs_options;                     /* 全局选项 */
struct newfs_super super;

/******************************************************************************
* SECTION: 工具函数声明
*******************************************************************************/
static int      newfs_mount(struct custom_options opt);
static int      newfs_umount(void);
static int      newfs_disk_read(off_t offset, void *buf, size_t size);
static int      newfs_disk_write(off_t offset, const void *buf, size_t size);
static int      newfs_block_read(uint32_t blkno, void *buf);
static int      newfs_block_write(uint32_t blkno, const void *buf);
static int      newfs_flush_inode_map(void);
static int      newfs_flush_data_map(void);
static int      newfs_read_inode(uint32_t ino, struct newfs_inode *inode);
static int      newfs_write_inode(const struct newfs_inode *inode);
static int      newfs_alloc_inode(void);
static int      newfs_alloc_data_block(void);
static int      newfs_prepare_root(void);
static int      newfs_load_dir_children(struct newfs_inode *dir,
                                        struct newfs_dentry *parent);
static struct newfs_dentry* newfs_find_child_dentry(const struct newfs_inode *dir,
                                                    const char *name);
static int      newfs_get_inode_from_dentry(struct newfs_dentry *dentry,
                                            struct newfs_inode **inode_out);
static int      newfs_path_dentry(const char *path, struct newfs_dentry **out);
static int      newfs_get_parent_dentry(const char *path, struct newfs_dentry **parent,
                                        char *child_name);
static void     newfs_link_child(struct newfs_inode *parent, struct newfs_dentry *child);
static void     newfs_free_dentry_tree(struct newfs_dentry *dentry);
static bool     bitmap_test(uint8_t *map, uint32_t idx);
static void     bitmap_set(uint8_t *map, uint32_t idx);
static void     bitmap_clear(uint8_t *map, uint32_t idx);
static int      newfs_lookup_in_dir(struct newfs_inode *dir, const char *name,
                                    struct newfs_dentry *dentry);
static int      newfs_add_dentry(struct newfs_inode *dir, const char *name,
                                 uint32_t ino, uint32_t mode,
                                 struct newfs_inode *child_inode);
static int      newfs_path_resolve(const char *path, struct newfs_inode *inode);
static int      newfs_get_parent(const char *path, struct newfs_inode *parent,
                                 char *child_name);
static int      newfs_load_super(struct newfs_super_d *disk_super);
static int      newfs_sync_super(const struct newfs_super_d *disk_super);

/******************************************************************************
* SECTION: FUSE操作定义
*******************************************************************************/
static struct fuse_operations operations = {
        .init = newfs_init,                                              /* mount文件系统 */
        .destroy = newfs_destroy,                                /* umount文件系统 */
        .mkdir = newfs_mkdir,                                    /* 建目录，mkdir */
        .getattr = newfs_getattr,                                /* 获取文件属性，类似stat，必须完成 */
        .readdir = newfs_readdir,                                /* 填充dentrys */
        .mknod = newfs_mknod,                                    /* 创建文件，touch相关 */
        .write = newfs_write,                                    /* 写入文件 */
        .read = newfs_read,                                     /* 读文件 */
        .utimens = newfs_utimens,                                /* 修改时间，忽略，避免touch报错 */
        .truncate = newfs_truncate,                              /* 改变文件大小 */
        .unlink = newfs_unlink,                                  /* 删除文件 */
        .rmdir  = newfs_rmdir,                                   /* 删除目录， rm -r */
        .rename = newfs_rename,                                  /* 重命名，mv */

        .open = newfs_open,
        .opendir = newfs_opendir,
        .access = newfs_access
};
/******************************************************************************
* SECTION: 必做函数实现
*******************************************************************************/
/**
 * @brief 挂载（mount）文件系统
 *
 * @param conn_info 可忽略，一些建立连接相关的信息
 * @return void*
 */
void* newfs_init(struct fuse_conn_info * conn_info) {
        (void)conn_info;
        if (newfs_mount(newfs_options) < 0) {
                fuse_exit(fuse_get_context()->fuse);
        }
        return NULL;
}

/**
 * @brief 卸载（umount）文件系统
 *
 * @param p 可忽略
 * @return void
 */
void newfs_destroy(void* p) {
        (void)p;
        newfs_umount();
        return;
}

/**
 * @brief 创建目录
 *
 * @param path 相对于挂载点的路径
 * @param mode 创建模式（只读？只写？），可忽略
 * @return int 0成功，否则返回对应错误号
 */
int newfs_mkdir(const char* path, mode_t mode) {
        (void)mode;
        struct newfs_dentry *parent_dentry = NULL;
        struct newfs_inode *parent_inode = NULL;
        struct newfs_inode new_inode;
        char name[MAX_NAME_LEN];
        int ret;

        ret = newfs_get_parent_dentry(path, &parent_dentry, name);
        if (ret < 0) {
                return ret;
        }

        ret = newfs_get_inode_from_dentry(parent_dentry, &parent_inode);
        if (ret < 0) {
                return ret;
        }

        if (!S_ISDIR(parent_inode->mode)) {
                return -ENOTDIR;
        }

        ret = newfs_load_dir_children(parent_inode, parent_dentry);
        if (ret < 0 && ret != -ENOTDIR) {
                return ret;
        }

        if (newfs_find_child_dentry(parent_inode, name)) {
                return -EEXIST;
        }

        ret = newfs_alloc_inode();
        if (ret < 0) {
                return ret;
        }

        struct newfs_inode *child_inode = calloc(1, sizeof(struct newfs_inode));
        if (!child_inode) {
                bitmap_clear(super.inode_map, (uint32_t)ret);
                newfs_flush_inode_map();
                return -ENOMEM;
        }

        memset(&new_inode, 0, sizeof(new_inode));
        new_inode.ino = (uint32_t)ret;
        new_inode.mode = S_IFDIR | NEWFS_DEFAULT_PERM;
        new_inode.links = 1;
        new_inode.size = 0;
        
        newfs_write_inode(&new_inode);

        *child_inode = new_inode;

        ret = newfs_add_dentry(parent_inode, name, new_inode.ino, new_inode.mode,
                               child_inode);
        if (ret < 0) {
                bitmap_clear(super.inode_map, new_inode.ino);
                newfs_flush_inode_map();
                free(child_inode);
                return ret;
        }

        return 0;
}

/**
 * @brief 获取文件或目录的属性，该函数非常重要
 *
 * @param path 相对于挂载点的路径
 * @param newfs_stat 返回状态
 * @return int 0成功，否则返回对应错误号
 */
int newfs_getattr(const char* path, struct stat * newfs_stat) {
        struct newfs_dentry *dentry = NULL;
        struct newfs_inode *inode = NULL;
        int ret = newfs_path_dentry(path, &dentry);
        if (ret < 0) {
                return ret;
        }
        ret = newfs_get_inode_from_dentry(dentry, &inode);
        if (ret < 0) {
                return ret;
        }

        memset(newfs_stat, 0, sizeof(struct stat));
        time_t now = time(NULL);
        newfs_stat->st_uid = getuid();
        newfs_stat->st_gid = getgid();
        newfs_stat->st_atime = now;
        newfs_stat->st_mtime = now;
        newfs_stat->st_ctime = now;
        newfs_stat->st_blksize = super.block_size;
        newfs_stat->st_mode = inode->mode;
        newfs_stat->st_ino = inode->ino;
        newfs_stat->st_size = inode->size;
        newfs_stat->st_blocks = (inode->size + super.io_size - 1) / super.io_size;
        newfs_stat->st_nlink = inode->links ? inode->links : 1;
        if (dentry == super.root_dentry) {
                newfs_stat->st_nlink = 2;
        }
        return 0;
}

/**
 * @brief 遍历目录项，填充至buf，并交给FUSE输出
 *
 * @param path 相对于挂载点的路径
 * @param buf 输出buffer
 * @param filler 参数讲解:
 *
 * typedef int (*fuse_fill_dir_t) (void *buf, const char *name,
 *                              const struct stat *stbuf, off_t off)
 * buf: name会被复制到buf中
 * name: dentry名字
 * stbuf: 文件状态，可忽略
 * off: 下一次offset从哪里开始，这里可以理解为第几个dentry
 *
 * @param offset 第几个目录项？
 * @param fi 可忽略
 * @return int 0成功，否则返回对应错误号
 */
int newfs_readdir(const char * path, void * buf, fuse_fill_dir_t filler, off_t offset,
                                         struct fuse_file_info * fi) {
    (void)fi;
    struct newfs_dentry *dir_dentry = NULL;
    struct newfs_inode *dir_inode = NULL;
    int ret = newfs_path_dentry(path, &dir_dentry);
    if (ret < 0) {
        return ret;
    }
    ret = newfs_get_inode_from_dentry(dir_dentry, &dir_inode);
    if (ret < 0) {
        return ret;
    }
    if (!S_ISDIR(dir_inode->mode)) {
        return -ENOTDIR;
    }

    ret = newfs_load_dir_children(dir_inode, dir_dentry);
    if (ret < 0 && ret != -ENOTDIR) {
        return ret;
    }

    uint32_t idx = 0;
    struct newfs_dentry *child = dir_inode->first_child;
    while (child) {
        if (idx >= (uint32_t)offset) {
            filler(buf, child->name, NULL, idx + 1);
        }
        child = child->brother;
        idx++;
    }
    return 0;
}

/**
 * @brief 创建文件
 *
 * @param path 相对于挂载点的路径
 * @param mode 创建文件的模式，可忽略
 * @param dev 设备类型，可忽略
 * @return int 0成功，否则返回对应错误号
 */
int newfs_mknod(const char* path, mode_t mode, dev_t dev) {
        (void)mode;
        (void)dev;
        struct newfs_dentry *parent_dentry = NULL;
        struct newfs_inode *parent_inode = NULL;
        struct newfs_inode inode;
        char name[MAX_NAME_LEN];
        int ret;

        ret = newfs_get_parent_dentry(path, &parent_dentry, name);
        if (ret < 0) {
                return ret;
        }
        ret = newfs_get_inode_from_dentry(parent_dentry, &parent_inode);
        if (ret < 0) {
                return ret;
        }
        if (!S_ISDIR(parent_inode->mode)) {
                return -ENOTDIR;
        }

        ret = newfs_load_dir_children(parent_inode, parent_dentry);
        if (ret < 0 && ret != -ENOTDIR) {
                return ret;
        }

        if (newfs_find_child_dentry(parent_inode, name)) {
                return -EEXIST;
        }

        ret = newfs_alloc_inode();
        if (ret < 0) {
                return ret;
        }

        struct newfs_inode *child_inode = calloc(1, sizeof(struct newfs_inode));
        if (!child_inode) {
                bitmap_clear(super.inode_map, (uint32_t)ret);
                newfs_flush_inode_map();
                return -ENOMEM;
        }

        memset(&inode, 0, sizeof(inode));
        inode.ino = (uint32_t)ret;
        inode.mode = S_IFREG | NEWFS_DEFAULT_PERM;
        inode.links = 1;
        inode.size = 0;
        newfs_write_inode(&inode);

        *child_inode = inode;

        ret = newfs_add_dentry(parent_inode, name, inode.ino, inode.mode, child_inode);
        if (ret < 0) {
                bitmap_clear(super.inode_map, inode.ino);
                newfs_flush_inode_map();
                free(child_inode);
                return ret;
        }

        return 0;
}

/**
 * @brief 修改时间，为了不让touch报错
 *
 * @param path 相对于挂载点的路径
 * @param tv 实践
 * @return int 0成功，否则返回对应错误号
 */
int newfs_utimens(const char* path, const struct timespec tv[2]) {
        (void)path;
        (void)tv;
        return 0;
}
/******************************************************************************
* SECTION: 选做函数实现
*******************************************************************************/
/**
 * @brief 写入文件
 *
 * @param path 相对于挂载点的路径
 * @param buf 写入的内容
 * @param size 写入的字节数
 * @param offset 相对文件的偏移
 * @param fi 可忽略
 * @return int 写入大小
 */
int newfs_write(const char* path, const char* buf, size_t size, off_t offset,
                        struct fuse_file_info* fi) {
        (void)path;
        (void)buf;
        (void)offset;
        (void)fi;
        /* 简化实现：不实际写入数据，直接返回 size 表示成功 */
        return (int)size;
}

/**
 * @brief 读取文件
 *
 * @param path 相对于挂载点的路径
 * @param buf 读取的内容
 * @param size 读取的字节数
 * @param offset 相对文件的偏移
 * @param fi 可忽略
 * @return int 读取大小
 */
int newfs_read(const char* path, char* buf, size_t size, off_t offset,
                       struct fuse_file_info* fi) {
        (void)path;
        (void)buf;
        (void)size;
        (void)offset;
        (void)fi;
        return 0;
}

/**
 * @brief 删除文件
 *
 * @param path 相对于挂载点的路径
 * @return int 0成功，否则返回对应错误号
 */
int newfs_unlink(const char* path) {
        (void)path;
        return -ENOSYS;
}

/**
 * @brief 删除目录
 *
 * @param path 相对于挂载点的路径
 * @return int 0成功，否则返回对应错误号
 */
int newfs_rmdir(const char* path) {
        (void)path;
        return -ENOSYS;
}

/**
 * @brief 重命名文件
 *
 * @param from 源文件路径
 * @param to 目标文件路径
 * @return int 0成功，否则返回对应错误号
 */
int newfs_rename(const char* from, const char* to) {
        (void)from;
        (void)to;
        return -ENOSYS;
}

/**
 * @brief 打开文件，可以在这里维护fi的信息，例如，fi->fh可以理解为一个64位指针，可以把自己想保存的数据结构
 * 保存在fh中
 *
 * @param path 相对于挂载点的路径
 * @param fi 文件信息
 * @return int 0成功，否则返回对应错误号
 */
int newfs_open(const char* path, struct fuse_file_info* fi) {
        (void)path;
        (void)fi;
        return 0;
}

/**
 * @brief 打开目录文件
 *
 * @param path 相对于挂载点的路径
 * @param fi 文件信息
 * @return int 0成功，否则返回对应错误号
 */
int newfs_opendir(const char* path, struct fuse_file_info* fi) {
        (void)path;
        (void)fi;
        return 0;
}

/**
 * @brief 改变文件大小
 *
 * @param path 相对于挂载点的路径
 * @param offset 改变后文件大小
 * @return int 0成功，否则返回对应错误号
 */
int newfs_truncate(const char* path, off_t offset) {
        (void)path;
        (void)offset;
        return 0;
}


/**
 * @brief 访问文件，因为读写文件时需要查看权限
 *
 * @param path 相对于挂载点的路径
 * @param type 访问类别
 * R_OK: Test for read permission.
 * W_OK: Test for write permission.
 * X_OK: Test for execute permission.
 * F_OK: Test for existence.
 *
 * @return int 0成功，否则返回对应错误号
 */
int newfs_access(const char* path, int type) {
        (void)type;
        struct newfs_inode inode;
        return newfs_path_resolve(path, &inode);
}
/******************************************************************************
* SECTION: FUSE入口
*******************************************************************************/
int main(int argc, char **argv)
{
    int ret;
        struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

        const char *home = getenv("HOME");
        if (home) {
                char path_buf[PATH_MAX];
                snprintf(path_buf, sizeof(path_buf), "%s/ddriver", home);
                newfs_options.device = strdup(path_buf);
        } else {
                newfs_options.device = strdup("ddriver");
        }

        if (fuse_opt_parse(&args, &newfs_options, option_spec, NULL) == -1)
                return -1;

        ret = fuse_main(args.argc, args.argv, &operations, NULL);
        fuse_opt_free_args(&args);
        return ret;
}

/******************************************************************************
* SECTION: 工具函数实现
*******************************************************************************/
static uint32_t newfs_inodes_per_block(void) {
        return super.block_size / sizeof(struct newfs_inode_d);
}

static int newfs_mount(struct custom_options opt){
        struct newfs_super_d disk_super;
        bool is_init = false;

        memset(&super, 0, sizeof(super));
        super.fd = ddriver_open((char *)opt.device);
        if (super.fd < 0) {
                return super.fd;
        }

        ddriver_ioctl(super.fd, IOC_REQ_DEVICE_IO_SZ, &super.io_size);
        ddriver_ioctl(super.fd, IOC_REQ_DEVICE_SIZE, &super.disk_size);

        super.block_size = NEWFS_BLOCK_SIZE;
        super.block_count = super.disk_size / super.block_size;

        if (super.block_count < 5) {
                return -ENOSPC;
        }

        if (newfs_disk_read(0, &disk_super, sizeof(disk_super)) < 0 ||
            disk_super.magic != NEWFS_MAGIC) {
                is_init = true;
                memset(&disk_super, 0, sizeof(disk_super));
        }

        if (is_init) {
                super.magic = NEWFS_MAGIC;
                super.sb_offset = 0;
                super.sb_blks = 1;

                super.ino_map_offset = super.sb_offset + super.sb_blks;
                super.ino_map_blks = 1;

                super.data_map_offset = super.ino_map_offset + super.ino_map_blks;
                super.data_map_blks = 1;

                super.inode_offset = super.data_map_offset + super.data_map_blks;
                super.inode_blks = 1;

                super.data_offset = super.inode_offset + super.inode_blks;
                super.data_blks = super.block_count - super.data_offset;

                uint32_t max_ino_bits = super.ino_map_blks * super.block_size * BITS_PER_BYTE;
                uint32_t max_data_bits = super.data_map_blks * super.block_size * BITS_PER_BYTE;

                super.inode_count = newfs_inodes_per_block() * super.inode_blks;
                if (super.inode_count > max_ino_bits) {
                        super.inode_count = max_ino_bits;
                }
                super.data_count = super.data_blks;
                if (super.data_count > max_data_bits) {
                        super.data_count = max_data_bits;
                        super.data_blks = super.data_count;
                }
                super.root_ino = 0;

                disk_super.magic = super.magic;
                disk_super.block_size = super.block_size;
                disk_super.sb_offset = super.sb_offset;
                disk_super.sb_blks = super.sb_blks;
                disk_super.ino_map_offset = super.ino_map_offset;
                disk_super.ino_map_blks = super.ino_map_blks;
                disk_super.data_map_offset = super.data_map_offset;
                disk_super.data_map_blks = super.data_map_blks;
                disk_super.inode_offset = super.inode_offset;
                disk_super.inode_blks = super.inode_blks;
                disk_super.data_offset = super.data_offset;
                disk_super.data_blks = super.data_blks;
                disk_super.inode_count = super.inode_count;
                disk_super.data_count = super.data_count;
                disk_super.root_ino = super.root_ino;
        } else {
                newfs_load_super(&disk_super);
        }

        super.inode_map = calloc(super.ino_map_blks, super.block_size);
        super.data_map = calloc(super.data_map_blks, super.block_size);
        if (!super.inode_map || !super.data_map) {
                return -ENOMEM;
        }

        if (!is_init) {
                for (uint32_t i = 0; i < super.ino_map_blks; i++) {
                        newfs_block_read(super.ino_map_offset + i,
                                          super.inode_map + i * super.block_size);
                }
                for (uint32_t i = 0; i < super.data_map_blks; i++) {
                        newfs_block_read(super.data_map_offset + i,
                                          super.data_map + i * super.block_size);
                }
                return newfs_prepare_root();
        }

        memset(super.inode_map, 0, super.ino_map_blks * super.block_size);
        memset(super.data_map, 0, super.data_map_blks * super.block_size);

        /* 分配根目录 */
        int root_ino = newfs_alloc_inode();
        struct newfs_inode root_inode;
        memset(&root_inode, 0, sizeof(root_inode));
        root_inode.ino = (uint32_t)root_ino;
        root_inode.mode = S_IFDIR | NEWFS_DEFAULT_PERM;
        root_inode.links = 1;
        root_inode.size = 0;
        newfs_write_inode(&root_inode);

        super.root_ino = (uint32_t)root_ino;
        disk_super.root_ino = super.root_ino;

        newfs_sync_super(&disk_super);
        newfs_flush_inode_map();
        newfs_flush_data_map();
        return newfs_prepare_root();
}

static int newfs_prepare_root(void){
        if (super.root_dentry) {
            newfs_free_dentry_tree(super.root_dentry);
            super.root_dentry = NULL;
        }

        struct newfs_dentry *root = calloc(1, sizeof(struct newfs_dentry));
        if (!root) {
                return -ENOMEM;
        }

        strncpy(root->name, "/", MAX_NAME_LEN - 1);
        root->ino = super.root_ino;
        root->mode = S_IFDIR | NEWFS_DEFAULT_PERM;
        super.root_dentry = root;

        struct newfs_inode *inode = NULL;
        int ret = newfs_get_inode_from_dentry(root, &inode);
        if (ret < 0) {
                newfs_free_dentry_tree(root);
                super.root_dentry = NULL;
                return ret;
        }

        return 0;
}

static int newfs_umount(void){
        if (super.root_dentry) {
                newfs_free_dentry_tree(super.root_dentry);
                super.root_dentry = NULL;
        }
        if (super.inode_map) {
                free(super.inode_map);
                super.inode_map = NULL;
        }
        if (super.data_map) {
                free(super.data_map);
                super.data_map = NULL;
        }
        if (super.fd > 0) {
                ddriver_close(super.fd);
                super.fd = -1;
        }
        return 0;
}

static int newfs_disk_read(off_t offset, void *buf, size_t size){
        if (size == 0) {
                return 0;
        }

        if (offset < 0) {
                return -EINVAL;
        }

        uint32_t io_sz = super.io_size;
        off_t down = round_down(offset, io_sz);
        off_t up = round_up(offset + (off_t)size, io_sz);

        if (up > (off_t)super.disk_size) {
                return -EIO;
        }

        size_t span = (size_t)(up - down);
        uint8_t *tmp = malloc(span);
        if (!tmp) {
                return -ENOMEM;
        }

        for (off_t pos = down; pos < up; pos += io_sz) {
                size_t buf_off = (size_t)(pos - down);
                if (ddriver_seek(super.fd, pos, SEEK_SET) < 0 ||
                    ddriver_read(super.fd, (char *)tmp + buf_off, io_sz) < 0) {
                        free(tmp);
                        return -EIO;
                }
        }

        size_t bias = (size_t)(offset - down);
        memcpy(buf, tmp + bias, size);

        free(tmp);
        return 0;
}

static int newfs_disk_write(off_t offset, const void *buf, size_t size){
        if (size == 0) {
                return 0;
        }

        if (offset < 0) {
                return -EINVAL;
        }

        uint32_t io_sz = super.io_size;
        off_t down = round_down(offset, io_sz);
        off_t up = round_up(offset + (off_t)size, io_sz);

        if (up > (off_t)super.disk_size) {
                return -ENOSPC;
        }

        size_t span = (size_t)(up - down);
        uint8_t *tmp = malloc(span);
        if (!tmp) {
                return -ENOMEM;
        }

        for (off_t pos = down; pos < up; pos += io_sz) {
                size_t buf_off = (size_t)(pos - down);
                if (ddriver_seek(super.fd, pos, SEEK_SET) < 0 ||
                    ddriver_read(super.fd, (char *)tmp + buf_off, io_sz) < 0) {
                        free(tmp);
                        return -EIO;
                }
        }

        size_t bias = (size_t)(offset - down);
        memcpy(tmp + bias, buf, size);

        for (off_t pos = down; pos < up; pos += io_sz) {
                size_t buf_off = (size_t)(pos - down);
                if (ddriver_seek(super.fd, pos, SEEK_SET) < 0 ||
                    ddriver_write(super.fd, (char *)tmp + buf_off, io_sz) < 0) {
                        free(tmp);
                        return -EIO;
                }
        }

        free(tmp);
        return 0;
}

static int newfs_block_read(uint32_t blkno, void *buf){
        uint32_t io_cnt = super.block_size / super.io_size;
        off_t base = (off_t)blkno * super.block_size;
        for (uint32_t i = 0; i < io_cnt; i++) {
                ddriver_seek(super.fd, base + i * super.io_size, SEEK_SET);
                if (ddriver_read(super.fd, (char *)buf + i * super.io_size, super.io_size) < 0) {
                        return -EIO;
                }
        }
        return 0;
}

static int newfs_block_write(uint32_t blkno, const void *buf){
        uint32_t io_cnt = super.block_size / super.io_size;
        off_t base = (off_t)blkno * super.block_size;
        for (uint32_t i = 0; i < io_cnt; i++) {
                ddriver_seek(super.fd, base + i * super.io_size, SEEK_SET);
                if (ddriver_write(super.fd, (char *)buf + i * super.io_size, super.io_size) < 0) {
                        return -EIO;
                }
        }
        return 0;
}

static bool bitmap_test(uint8_t *map, uint32_t idx){
        return (map[idx / BITS_PER_BYTE] >> (idx % BITS_PER_BYTE)) & 0x1;
}

static void bitmap_set(uint8_t *map, uint32_t idx){
        map[idx / BITS_PER_BYTE] |= (1 << (idx % BITS_PER_BYTE));
}

static void bitmap_clear(uint8_t *map, uint32_t idx){
        map[idx / BITS_PER_BYTE] &= ~(1 << (idx % BITS_PER_BYTE));
}

static int newfs_flush_inode_map(void){
        for (uint32_t i = 0; i < super.ino_map_blks; i++) {
                newfs_block_write(super.ino_map_offset + i, super.inode_map + i * super.block_size);
        }
        return 0;
}

static int newfs_flush_data_map(void){
        for (uint32_t i = 0; i < super.data_map_blks; i++) {
                newfs_block_write(super.data_map_offset + i, super.data_map + i * super.block_size);
        }
        return 0;
}

static int newfs_alloc_inode(void){
        for (uint32_t i = 0; i < super.inode_count; i++) {
                if (!bitmap_test(super.inode_map, i)) {
                        bitmap_set(super.inode_map, i);
                        newfs_flush_inode_map();
                        struct newfs_inode_d zero;
                        memset(&zero, 0, sizeof(zero));
                        uint32_t blk = super.inode_offset + i / newfs_inodes_per_block();
                        uint32_t off = (i % newfs_inodes_per_block()) * sizeof(struct newfs_inode_d);
                        char buf[NEWFS_BLOCK_SIZE];
                        newfs_block_read(blk, buf);
                        memcpy(buf + off, &zero, sizeof(zero));
                        newfs_block_write(blk, buf);
                        return (int)i;
                }
        }
        return -ENOSPC;
}

static int newfs_alloc_data_block(void){
        for (uint32_t i = 0; i < super.data_count; i++) {
                if (!bitmap_test(super.data_map, i)) {
                        bitmap_set(super.data_map, i);
                        newfs_flush_data_map();
                        char zero[NEWFS_BLOCK_SIZE];
                        memset(zero, 0, sizeof(zero));
                        newfs_block_write(super.data_offset + i, zero);
                        return (int)(super.data_offset + i);
                }
        }
        return -ENOSPC;
}

static int newfs_read_inode(uint32_t ino, struct newfs_inode *inode){
        if (ino >= super.inode_count) {
                return -EINVAL;
        }
        uint32_t blk = super.inode_offset + ino / newfs_inodes_per_block();
        uint32_t off = (ino % newfs_inodes_per_block()) * sizeof(struct newfs_inode_d);
        char buf[NEWFS_BLOCK_SIZE];
        struct newfs_inode_d disk_inode;
        newfs_block_read(blk, buf);
        memcpy(&disk_inode, buf + off, sizeof(disk_inode));

        inode->ino = ino;
        inode->mode = disk_inode.mode;
        inode->size = disk_inode.size;
        inode->links = disk_inode.links;
        memcpy(inode->blocks, disk_inode.blocks, sizeof(disk_inode.blocks));
        inode->dentry = NULL;
        inode->first_child = NULL;
        inode->data = NULL;
        inode->children_loaded = false;
        return 0;
}

static int newfs_write_inode(const struct newfs_inode *inode){
        if (inode->ino >= super.inode_count) {
                return -EINVAL;
        }
        uint32_t blk = super.inode_offset + inode->ino / newfs_inodes_per_block();
        uint32_t off = (inode->ino % newfs_inodes_per_block()) * sizeof(struct newfs_inode_d);
        char buf[NEWFS_BLOCK_SIZE];
        struct newfs_inode_d disk_inode;
        newfs_block_read(blk, buf);
        disk_inode.mode = inode->mode;
        disk_inode.size = inode->size;
        disk_inode.links = inode->links;
        memcpy(disk_inode.blocks, inode->blocks, sizeof(disk_inode.blocks));
        memcpy(buf + off, &disk_inode, sizeof(disk_inode));
        newfs_block_write(blk, buf);
        return 0;
}

static void newfs_link_child(struct newfs_inode *parent, struct newfs_dentry *child){
        if (!parent || !child) {
                return;
        }

        child->brother = parent->first_child;
        parent->first_child = child;
}

static struct newfs_dentry* newfs_find_child_dentry(const struct newfs_inode *dir,
                                                    const char *name){
        if (!dir || !name) {
                return NULL;
        }
        struct newfs_dentry *iter = dir->first_child;
        while (iter) {
                if (strncmp(iter->name, name, MAX_NAME_LEN) == 0) {
                        return iter;
                }
                iter = iter->brother;
        }
        return NULL;
}

static int newfs_load_dir_children(struct newfs_inode *dir, struct newfs_dentry *parent){
        if (!dir || !S_ISDIR(dir->mode)) {
                return -ENOTDIR;
        }

        if (dir->children_loaded) {
                return 0;
        }

        dir->first_child = NULL;

        uint32_t entry_cnt = dir->size / sizeof(struct newfs_dentry_d);
        for (uint32_t i = 0; i < entry_cnt; i++) {
                uint32_t blk_idx = (i * sizeof(struct newfs_dentry_d)) / super.block_size;
                uint32_t blk_off = (i * sizeof(struct newfs_dentry_d)) % super.block_size;
                if (blk_idx >= NEWFS_DIRECT_NUM || dir->blocks[blk_idx] == 0) {
                        continue;
                }
                char buf[NEWFS_BLOCK_SIZE];
                if (newfs_block_read(dir->blocks[blk_idx], buf) < 0) {
                        continue;
                }
                struct newfs_dentry_d disk_dentry;
                memcpy(&disk_dentry, buf + blk_off, sizeof(disk_dentry));

                struct newfs_dentry *child = calloc(1, sizeof(struct newfs_dentry));
                if (!child) {
                        struct newfs_dentry *cleanup = dir->first_child;
                        while (cleanup) {
                                struct newfs_dentry *next = cleanup->brother;
                                newfs_free_dentry_tree(cleanup);
                                cleanup = next;
                        }
                        dir->first_child = NULL;
                        return -ENOMEM;
                }
                strncpy(child->name, disk_dentry.name, MAX_NAME_LEN - 1);
                child->ino = disk_dentry.ino;
                child->mode = disk_dentry.mode;
                child->parent = parent;
                child->brother = NULL;
                child->inode = NULL;
                newfs_link_child(dir, child);
        }

        dir->children_loaded = true;
        return 0;
}

static int newfs_get_inode_from_dentry(struct newfs_dentry *dentry,
                                            struct newfs_inode **inode_out){
        if (!dentry) {
                return -ENOENT;
        }
        if (dentry->inode) {
                if (inode_out) {
                        *inode_out = dentry->inode;
                }
                return 0;
        }

        struct newfs_inode *inode = calloc(1, sizeof(struct newfs_inode));
        if (!inode) {
                return -ENOMEM;
        }
        int ret = newfs_read_inode(dentry->ino, inode);
        if (ret < 0) {
                free(inode);
                return ret;
        }

        inode->dentry = dentry;
        dentry->inode = inode;

        if (S_ISDIR(inode->mode)) {
                ret = newfs_load_dir_children(inode, dentry);
                if (ret < 0 && ret != -ENOTDIR) {
                        dentry->inode = NULL;
                        free(inode);
                        return ret;
                }
        }

        if (inode_out) {
                *inode_out = inode;
        }
        return 0;
}

static void newfs_free_dentry_tree(struct newfs_dentry *dentry){
        if (!dentry) {
                return;
        }

        struct newfs_inode *inode = dentry->inode;
        struct newfs_dentry *child = NULL;
        if (inode) {
                child = inode->first_child;
                inode->first_child = NULL;
        }

        while (child) {
                struct newfs_dentry *next = child->brother;
                child->brother = NULL;
                newfs_free_dentry_tree(child);
                child = next;
        }

        if (inode) {
                free(inode->data);
                free(inode);
        }

        free(dentry);
}

static int newfs_lookup_in_dir(struct newfs_inode *dir, const char *name,
                                    struct newfs_dentry *dentry){
        if (!S_ISDIR(dir->mode)) {
                return -ENOTDIR;
        }
        if (!name || strlen(name) == 0) {
                return -EINVAL;
        }

        if (dir->dentry) {
                if (!dir->children_loaded) {
                        newfs_load_dir_children(dir, dir->dentry);
                }
                struct newfs_dentry *child = newfs_find_child_dentry(dir, name);
                if (child) {
                        if (dentry) {
                                strncpy(dentry->name, child->name, MAX_NAME_LEN);
                                dentry->ino = child->ino;
                                dentry->mode = child->mode;
                        }
                        return 0;
                }
        }
        uint32_t cnt = dir->size / sizeof(struct newfs_dentry_d);
        char buf[NEWFS_BLOCK_SIZE];
        struct newfs_dentry_d tmp;
        for (uint32_t i = 0; i < cnt; i++) {
                uint32_t blk_idx = (i * sizeof(struct newfs_dentry_d)) / super.block_size;
                uint32_t blk_off = (i * sizeof(struct newfs_dentry_d)) % super.block_size;
                if (blk_idx >= NEWFS_DIRECT_NUM || dir->blocks[blk_idx] == 0) {
                        continue;
                }
                if (newfs_block_read(dir->blocks[blk_idx], buf) < 0) {
                        continue;
                }
                memcpy(&tmp, buf + blk_off, sizeof(tmp));
                if (strncmp(tmp.name, name, MAX_NAME_LEN) == 0) {
                        if (dentry) {
                                strncpy(dentry->name, tmp.name, MAX_NAME_LEN);
                                dentry->ino = tmp.ino;
                                dentry->mode = tmp.mode;
                        }
                        return 0;
                }
        }
        return -ENOENT;
}

static int newfs_add_dentry(struct newfs_inode *dir, const char *name,
                                 uint32_t ino, uint32_t mode,
                                 struct newfs_inode *child_inode){
        if (!S_ISDIR(dir->mode)) {
                return -ENOTDIR;
        }

        if (dir->dentry && !dir->children_loaded) {
                newfs_load_dir_children(dir, dir->dentry);
        }

        struct newfs_dentry *child = NULL;
        if (dir->dentry || child_inode) {
                child = calloc(1, sizeof(struct newfs_dentry));
                if (!child) {
                        return -ENOMEM;
                }
                strncpy(child->name, name, MAX_NAME_LEN - 1);
                child->ino = ino;
                child->mode = mode;
                child->parent = dir->dentry;
                child->inode = child_inode;
                if (child_inode) {
                        child_inode->dentry = child;
                }
        }

        struct newfs_dentry_d entry;
        memset(&entry, 0, sizeof(entry));
        strncpy(entry.name, name, MAX_NAME_LEN - 1);
        entry.ino = ino;
        entry.mode = mode;

        uint32_t idx = dir->size / sizeof(struct newfs_dentry_d);
        uint32_t blk_idx = (idx * sizeof(struct newfs_dentry_d)) / super.block_size;
        uint32_t blk_off = (idx * sizeof(struct newfs_dentry_d)) % super.block_size;

        if (blk_idx >= NEWFS_DIRECT_NUM) {
                return -ENOSPC;
        }

        if (dir->blocks[blk_idx] == 0) {
                int new_blk = newfs_alloc_data_block();
                if (new_blk < 0) {
                        return new_blk;
                }
                dir->blocks[blk_idx] = (uint32_t)new_blk;
        }

        char buf[NEWFS_BLOCK_SIZE];
        newfs_block_read(dir->blocks[blk_idx], buf);
        memcpy(buf + blk_off, &entry, sizeof(entry));
        newfs_block_write(dir->blocks[blk_idx], buf);

        dir->size += sizeof(struct newfs_dentry_d);
        newfs_write_inode(dir);

        if (child) {
                newfs_link_child(dir, child);
                dir->children_loaded = true;
        }
        return 0;
}

static int newfs_path_dentry(const char *path, struct newfs_dentry **out){
        if (!path || !super.root_dentry) {
                return -ENOENT;
        }

        if (strcmp(path, "/") == 0) {
                if (out) {
                        *out = super.root_dentry;
                }
                return 0;
        }

        char tmp[PATH_MAX];
        char *token;
        char *saveptr;
        strncpy(tmp, path, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';

        struct newfs_dentry *cur = super.root_dentry;
        struct newfs_inode *cur_inode = NULL;
        int ret = newfs_get_inode_from_dentry(cur, &cur_inode);
        if (ret < 0) {
                return ret;
        }

        token = strtok_r(tmp, "/", &saveptr);
        while (token) {
                ret = newfs_load_dir_children(cur_inode, cur);
                if (ret < 0 && ret != -ENOTDIR) {
                        return ret;
                }
                struct newfs_dentry *child = newfs_find_child_dentry(cur_inode, token);
                if (!child) {
                        return -ENOENT;
                }
                cur = child;
                ret = newfs_get_inode_from_dentry(cur, &cur_inode);
                if (ret < 0) {
                        return ret;
                }
                token = strtok_r(NULL, "/", &saveptr);
        }

        if (out) {
                *out = cur;
        }
        return 0;
}

static int newfs_get_parent_dentry(const char *path, struct newfs_dentry **parent,
                                        char *child_name){
        if (strcmp(path, "/") == 0) {
                return -EEXIST;
        }

        char tmp[PATH_MAX];
        char *token;
        char *saveptr;
        strncpy(tmp, path, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';

        struct newfs_dentry *cur = super.root_dentry;
        struct newfs_inode *cur_inode = NULL;
        int ret = newfs_get_inode_from_dentry(cur, &cur_inode);
        if (ret < 0) {
                return ret;
        }

        token = strtok_r(tmp, "/", &saveptr);
        while (token) {
                char *next = strtok_r(NULL, "/", &saveptr);
                if (!next) {
                        if (child_name) {
                                strncpy(child_name, token, MAX_NAME_LEN - 1);
                                child_name[MAX_NAME_LEN - 1] = '\0';
                        }
                        if (parent) {
                                *parent = cur;
                        }
                        return 0;
                }

                ret = newfs_load_dir_children(cur_inode, cur);
                if (ret < 0 && ret != -ENOTDIR) {
                        return ret;
                }
                struct newfs_dentry *child = newfs_find_child_dentry(cur_inode, token);
                if (!child) {
                        return -ENOENT;
                }
                cur = child;
                ret = newfs_get_inode_from_dentry(cur, &cur_inode);
                if (ret < 0) {
                        return ret;
                }
                token = next;
        }

        return -ENOENT;
}

static int newfs_path_resolve(const char *path, struct newfs_inode *inode){
        struct newfs_dentry *dentry = NULL;
        struct newfs_inode *found = NULL;
        int ret = newfs_path_dentry(path, &dentry);
        if (ret < 0) {
                return ret;
        }

        ret = newfs_get_inode_from_dentry(dentry, &found);
        if (ret < 0) {
                return ret;
        }

        if (inode) {
                *inode = *found;
                inode->dentry = NULL;
                inode->first_child = NULL;
                inode->data = NULL;
                inode->children_loaded = false;
        }
        return 0;
}

static int newfs_get_parent(const char *path, struct newfs_inode *parent,
                                 char *child_name){
        struct newfs_dentry *parent_dentry = NULL;
        struct newfs_inode *parent_inode = NULL;
        int ret = newfs_get_parent_dentry(path, &parent_dentry, child_name);
        if (ret < 0) {
                return ret;
        }

        ret = newfs_get_inode_from_dentry(parent_dentry, &parent_inode);
        if (ret < 0) {
                return ret;
        }

        if (parent) {
                *parent = *parent_inode;
                parent->dentry = NULL;
                parent->first_child = NULL;
                parent->data = NULL;
                parent->children_loaded = false;
        }
        return 0;
}

static int newfs_load_super(struct newfs_super_d *disk_super){
        super.block_size = disk_super->block_size;
        super.magic = disk_super->magic;
        super.sb_offset = disk_super->sb_offset;
        super.sb_blks = disk_super->sb_blks;
        super.ino_map_offset = disk_super->ino_map_offset;
        super.ino_map_blks = disk_super->ino_map_blks;
        super.data_map_offset = disk_super->data_map_offset;
        super.data_map_blks = disk_super->data_map_blks;
        super.inode_offset = disk_super->inode_offset;
        super.inode_blks = disk_super->inode_blks;
        super.data_offset = disk_super->data_offset;
        super.data_blks = disk_super->data_blks;
        super.inode_count = disk_super->inode_count;
        super.data_count = disk_super->data_count;
        super.root_ino = disk_super->root_ino;
        return 0;
}

static int newfs_sync_super(const struct newfs_super_d *disk_super){
        return newfs_disk_write(0, disk_super, sizeof(*disk_super));
}

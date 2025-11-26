#ifndef _TYPES_H_
#define _TYPES_H_

#include <stdint.h>
#include <stdbool.h>

#define MAX_NAME_LEN    128

struct custom_options {
        const char*        device;
};

struct newfs_super {
    uint32_t magic;
    int      fd;
    uint32_t io_size;             /* 设备IO大小 */
    uint32_t block_size;          /* 逻辑块大小 */
    uint32_t disk_size;           /* 设备大小 */
    uint32_t block_count;         /* 总逻辑块数 */

    /* 磁盘布局 */
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

    uint8_t*  inode_map;
    uint8_t*  data_map;

    struct newfs_dentry* root_dentry;
};

struct newfs_inode {
    uint32_t ino;
    uint32_t mode;
    uint32_t size;
    uint32_t links;
    uint32_t blocks[8];

    struct newfs_dentry* dentry;
    struct newfs_dentry* first_child;
    uint8_t* data;
    bool children_loaded;
};

struct newfs_dentry {
    char     name[MAX_NAME_LEN];
    uint32_t ino;
    uint32_t mode;

    struct newfs_dentry* parent;
    struct newfs_dentry* brother;
    struct newfs_inode*  inode;
};

#endif /* _TYPES_H_ */
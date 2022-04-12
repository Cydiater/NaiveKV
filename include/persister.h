#pragma once
#ifndef INCLUDE_PERSISTER_H
#define INCLUDE_PERSISTER_H

#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <string>

namespace kvs
{
namespace real_storage
{
class File
{
public:
    File(int fd, const std::string &path_name) : fd_(fd), path_name_(path_name)
    {
    }
    bool valid() const
    {
        return fd_ > 0 && !closed_;
    }
    ssize_t write(const void *buf, size_t count)
    {
        return ::write(fd_, buf, count);
    }
    ssize_t read(void *buf, size_t count)
    {
        return ::read(fd_, buf, count);
    }
    off_t lseek(off_t offset, int whence)
    {
        return ::lseek(fd_, offset, whence);
    }
    const char *pathname() const
    {
        return path_name_.c_str();
    }
    int close()
    {
        return ::close(fd_);
    }

private:
    int fd_;
    std::string path_name_;
    bool closed_{false};
};

class FilePool
{
public:
    FilePool(const std::string &dir) : directory_(dir)
    {
        if (directory_.size() == 0)
        {
            std::cerr << "empty directory.";
            throw std::runtime_error("Invalid directory");
        }
        if (directory_[directory_.size() - 1] != '/')
        {
            directory_.push_back('/');
        }
    }

    File open(const char *pathname, bool o_trunc, bool o_append, bool o_creat)
    {
        auto actual_path = directory_ + pathname;
        int flag = O_RDWR;
        if (o_append)
        {
            flag |= O_CREAT;
        }
        if (o_trunc)
        {
            flag |= O_TRUNC;
        }
        if (o_creat)
        {
            flag |= O_CREAT;
        }
        int ret_fd = ::open(
            actual_path.c_str(), flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (ret_fd <= 0)
        {
            std::cerr << "Failed to open file " << pathname
                      << ". errno: " << errno;
        }
        return File(ret_fd, actual_path);
    }

    int unlink(const File &file)
    {
        return ::unlink(file.pathname());
    }

private:
    std::string directory_;
};
}  // namespace real_storage
}  // namespace kvs

#endif
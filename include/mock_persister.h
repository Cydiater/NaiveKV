#pragma once
#ifndef INCLUDE_MOCK_PERSISTER_H
#define INCLUDE_MOCK_PERSISTER_H

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <array>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvs
{
namespace mock_storage
{
class FilePool;

class FileStorage
{
public:
    using Pointer = std::shared_ptr<FileStorage>;
    ssize_t write(off_t position, const char *buf, size_t count)
    {
        std::lock_guard<std::mutex> lk(mu_);
        return do_write(position, buf, count);
    }
    ssize_t read(off_t position, char *buf, size_t count)
    {
        std::lock_guard<std::mutex> lk(mu_);
        return do_read(position, buf, count);
    }
    int fsync()
    {
        std::lock_guard<std::mutex> lk(mu_);

        on_disk_ = in_memory_;
        return 0;
    }

    ssize_t append(const char *buf, size_t count)
    {
        std::lock_guard<std::mutex> lk(mu_);
        return do_write(file_size_, buf, count);
    }
    size_t size() const
    {
        return file_size_;
    }

private:
    friend class FilePool;
    void crash()
    {
        in_memory_ = on_disk_;
    }
    ssize_t do_write(off_t position, const char *buf, size_t count)
    {
        size_t size_at_least = position + count;
        if (size_at_least > (size_t) file_size_)
        {
            in_memory_.resize(size_at_least);
            on_disk_.resize(size_at_least);
            file_size_ = size_at_least;
        }
        memcpy(in_memory_.data() + position, buf, count);
        return count;
    }
    ssize_t do_read(off_t position, char *buf, size_t count)
    {
        if (position >= off_t(file_size_))
        {
            return 0;
        }
        count = std::min((ssize_t) count, (ssize_t) (file_size_ - position));
        memcpy(buf, in_memory_.data() + position, count);
        return count;
    }

    std::vector<char> in_memory_;
    std::vector<char> on_disk_;
    size_t file_size_{0};
    std::mutex mu_;
};
class File
{
public:
    File(int fd,
         const std::string &path_name,
         FileStorage::Pointer file_storage)
        : fd_(fd), path_name_(path_name), file_storage_(file_storage)
    {
        mu_ = std::make_unique<std::mutex>();
    }
    bool valid() const
    {
        return fd_ > 0 && !closed_;
    }
    ssize_t write(const void *buf, size_t count)
    {
        std::lock_guard<std::mutex> lk(*mu_);

        if (fd_ <= 0 || closed_)
        {
            errno = EBADF;
            return -1;
        }
        if (append_)
        {
            // TODO: handle offset here?
            return file_storage_->append((const char *) buf, count);
        }
        else
        {
            auto ret = file_storage_->write(cursor_, (const char *) buf, count);
            cursor_ += ret;
            return ret;
        }
    }
    ssize_t read(void *buf, size_t count)
    {
        std::lock_guard<std::mutex> lk(*mu_);

        if (fd_ <= 0 || closed_)
        {
            errno = EBADF;
            return -1;
        }
        if (append_)
        {
            return 0;
        }
        else
        {
            auto ret = file_storage_->read(cursor_, (char *) buf, count);
            cursor_ += ret;
            // std::cout << "[debug] read(" << cursor_ << ", buf, " << count <<
            // "), ret: " << ret << std::endl;
            return ret;
        }
    }
    // TODO: what is the behavor of lseek to o_append file?
    off_t lseek(off_t offset, int whence)
    {
        std::lock_guard<std::mutex> lk(*mu_);

        if (whence == SEEK_SET)
        {
            cursor_ = offset;
        }
        else if (whence == SEEK_CUR)
        {
            cursor_ += offset;
        }
        else if (whence == SEEK_END)
        {
            cursor_ = (off_t) file_storage_->size() + offset;
        }
        else
        {
            errno = EINVAL;
            return (off_t) -1;
        }
        return cursor_;
    }
    const char *pathname() const
    {
        return path_name_.c_str();
    }
    int close()
    {
        closed_ = true;
        return 0;
    }

private:
    friend class FilePool;

    int fd_;
    std::string path_name_;
    FileStorage::Pointer file_storage_;
    bool closed_{false};
    bool append_{false};
    off_t cursor_{0};

    std::unique_ptr<std::mutex> mu_;
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
        std::lock_guard<std::mutex> lk(mu_);

        auto actual_path = directory_ + pathname;
        // File ret_file(allocated_fd_++, actual_path);

        if (o_trunc)
        {
            if (file_storages_.count(actual_path) == 1)
            {
                auto f = file_storages_[actual_path];
                f->in_memory_.clear();
                f->on_disk_.clear();
                f->file_size_ = 0;
            }
        }

        FileStorage::Pointer ret_file_storage;
        if (file_storages_.count(actual_path) == 0)
        {
            if (o_creat)
            {
                file_storages_[actual_path] = std::make_shared<FileStorage>();
                ret_file_storage = file_storages_[actual_path];
            }
            else
            {
                errno = ENOENT;
                return File(-1, actual_path, nullptr);
            }
        }
        else
        {
            ret_file_storage = file_storages_[actual_path];
        }

        File ret_file(allocated_fd_++, actual_path, ret_file_storage);
        if (o_append)
        {
            ret_file.append_ = true;
            ret_file.cursor_ = ret_file_storage->size();
        }
        return ret_file;
    }

    int unlink(File *file)
    {
        std::lock_guard<std::mutex> lk(mu_);

        if (file_storages_.count(file->pathname()) == 1)
        {
            file_storages_.erase(file->pathname());
            return 0;
        }
        errno = ENOENT;
        return -1;
    }

private:
    std::string directory_;
    size_t allocated_fd_{3};

    std::mutex mu_;
    using FileName = std::string;
    std::unordered_map<FileName, FileStorage::Pointer> file_storages_;
};
}  // namespace mock_storage
}  // namespace kvs

#endif
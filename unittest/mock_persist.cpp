#include "mock_persister.h"
#include "persister.h"
#include "util/utils.h"

using namespace kvs;

constexpr static size_t kBuffLimit = 102400;
char input_buf[kBuffLimit];
char real_read_buf[kBuffLimit];
char mock_read_buf[kBuffLimit];

void assert_offset_same(real_storage::File &r, mock_storage::File &m)
{
    auto r_off = r.lseek(0, SEEK_CUR);
    auto m_off = m.lseek(0, SEEK_CUR);
    if (r_off != m_off)
    {
        std::cerr << "offset mismatch. real " << r_off << " v.s. " << m_off
                  << std::endl;
        throw std::runtime_error("Offset Mismatch");
    }
    else
    {
        std::cout << "offset match at " << r_off << std::endl;
    }
}

int main()
{
    real_storage::FilePool real_pool("./tmp_unittest/");
    mock_storage::FilePool mock_pool("./tmp_unittest/");

    auto real_f = real_pool.open("tmp_real", true, false, true);
    auto mock_f = mock_pool.open("tmp_real", true, false, true);
    if (!real_f.valid())
    {
        std::cerr << "failed to open real file. errno: " << errno << std::endl;
        return -1;
    }

    if (!mock_f.valid())
    {
        std::cerr << "failed to open mock file" << std::endl;
        return -1;
    }

    std::cout << "gen..." << std::endl;
    bench::gen_random(input_buf, kBuffLimit);
    std::cout << "done..." << std::endl;

    for (size_t test_time = 0; test_time < 100; ++test_time)
    {
        auto op = rand() % 3;
        switch (op)
        {
        case 0:
        {
            // write
            auto offset = rand() % kBuffLimit;
            auto len = rand() % (kBuffLimit - offset);
            size_t actual_write = 0;
            while (actual_write < len)
            {
                auto ret =
                    real_f.write(input_buf + actual_write, len - actual_write);
                if (ret < 0)
                {
                    std::cerr << "Failed to write. errno: " << errno
                              << std::endl;
                    return -1;
                }
                actual_write += ret;
            }
            mock_f.write(input_buf, len);

            std::cout << "[bench] write " << len << std::endl;
            assert_offset_same(real_f, mock_f);

            break;
        }
        case 1:
        {
            // read
            auto offset = rand() % kBuffLimit;
            auto len = rand() % (kBuffLimit - offset);

            size_t actual_read = 0;
            while (actual_read < len)
            {
                auto ret =
                    real_f.read(real_read_buf + actual_read, len - actual_read);
                if (ret < 0)
                {
                    std::cerr << "Failed to read. errno: " << errno
                              << std::endl;
                    return -1;
                }
                if (ret == 0)
                {
                    break;
                }
                actual_read += ret;
            }
            size_t mock_read = mock_f.read(mock_read_buf, len);
            if (actual_read != mock_read)
            {
                std::cerr << "Read bytes_nr mismatch at " << test_time
                          << ": real " << actual_read << ", mock: " << mock_read
                          << std::endl;
                return -1;
            }

            auto ret = memcmp(mock_read_buf, real_read_buf, actual_read);
            if (ret != 0)
            {
                std::cerr << "Read mismatch at " << test_time
                          << " cases. real: " << actual_read
                          << " bytes, mock: " << mock_read
                          << " bytes. len: " << len << std::endl;
                return -1;
            }

            std::cout << "[bench] read " << len << ", actual get "
                      << actual_read << std::endl;
            assert_offset_same(real_f, mock_f);

            break;
        }
        case 2:
        {
            // lseek
            auto offset = rand() % kBuffLimit;
            auto whence_op = rand() % 3;
            int whence = 0;
            if (whence_op == 0)
            {
                whence = SEEK_SET;
            }
            else if (whence_op == 1)
            {
                whence = SEEK_CUR;
            }
            else
            {
                whence = SEEK_END;
            }

            auto ret_real = real_f.lseek(offset, whence);
            auto ret_mock = mock_f.lseek(offset, whence);
            if (ret_real != ret_mock)
            {
                std::cerr << "lseek mismatch. real: " << ret_real
                          << " v.s. mock: " << ret_mock
                          << " for offset = " << offset
                          << ", whence = " << whence << std::endl;
                return -1;
            }
            std::cout << "[bench] lseek " << offset << ", " << whence
                      << std::endl;
            assert_offset_same(real_f, mock_f);

            break;
        }
        default:
        {
            std::cerr << "Err: invalid op " << op << std::endl;
            throw std::runtime_error("Invalid Op");
        }
        }
    }
}
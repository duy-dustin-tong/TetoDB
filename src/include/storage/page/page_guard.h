#pragma once

#include "storage/page/page.h"
#include "storage/buffer/buffer_pool_manager.h"

namespace tetodb {

    class WritePageGuard {
    public:
        // 1. Default Constructor
        WritePageGuard() = default;

        // 2. Resource Constructor
        WritePageGuard(BufferPoolManager* bpm, Page* page)
            : bpm_(bpm), page_(page) {
            if (page_) {
                page_->WLatch();
            }
        }

        // 3. Move Constructor
        // Take the pointer from 'other', set 'other' to null.
        WritePageGuard(WritePageGuard&& other) noexcept
            : bpm_(other.bpm_), page_(other.page_), is_dirty_(other.is_dirty_) {
            other.page_ = nullptr;
            other.bpm_ = nullptr;
        }

        // 4. Move Assignment Operator (CRITICAL FIX)
        WritePageGuard& operator=(WritePageGuard&& other) noexcept {
            if (this != &other) {
                // First, release whatever WE are currently holding
                Drop();

                // Now take over the new resources
                bpm_ = other.bpm_;
                page_ = other.page_;
                is_dirty_ = other.is_dirty_;

                // Nullify the other so it doesn't double-free
                other.page_ = nullptr;
                other.bpm_ = nullptr;
            }
            return *this;
        }

        // Disable Copy
        WritePageGuard(const WritePageGuard&) = delete;
        WritePageGuard& operator=(const WritePageGuard&) = delete;

        // Destructor
        ~WritePageGuard() { Drop(); }

        // --- Casting Fix ---
        template <class T>
        T* As() {
            // FIX: Cast the Page Object, NOT the data buffer
            return static_cast<T*>(page_);
        }

        void MarkDirty() { is_dirty_ = true; }

        void Drop() {
            if (page_) {
                page_->WUnlatch();
                bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
                page_ = nullptr;
            }
        }

    private:
        BufferPoolManager* bpm_{ nullptr };
        Page* page_{ nullptr };
        bool is_dirty_{ false };
    };

    /**
     * ReadPageGuard
     */
    class ReadPageGuard {
    public:
        ReadPageGuard() = default;

        ReadPageGuard(BufferPoolManager* bpm, Page* page)
            : bpm_(bpm), page_(page) {
            if (page_) {
                page_->RLatch();
            }
        }

        // Move Constructor
        ReadPageGuard(ReadPageGuard&& other) noexcept
            : bpm_(other.bpm_), page_(other.page_) {
            other.page_ = nullptr;
            other.bpm_ = nullptr;
        }

        // Move Assignment
        ReadPageGuard& operator=(ReadPageGuard&& other) noexcept {
            if (this != &other) {
                Drop();
                bpm_ = other.bpm_;
                page_ = other.page_;
                other.page_ = nullptr;
                other.bpm_ = nullptr;
            }
            return *this;
        }

        ReadPageGuard(const ReadPageGuard&) = delete;
        ReadPageGuard& operator=(const ReadPageGuard&) = delete;

        ~ReadPageGuard() { Drop(); }

        template <class T>
        T* As() {
            return static_cast<T*>(page_);
        }

        void Drop() {
            if (page_) {
                page_->RUnlatch();
                bpm_->UnpinPage(page_->GetPageId(), false);
                page_ = nullptr;
            }
        }

    private:
        BufferPoolManager* bpm_{ nullptr };
        Page* page_{ nullptr };
    };

} // namespace tetodb
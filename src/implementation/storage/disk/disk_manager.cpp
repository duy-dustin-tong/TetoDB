// disk_manager.cpp


#include <iostream>
#include <stdexcept>
#include "storage/disk/disk_manager.h"

namespace tetodb {

	DiskManager::DiskManager(std::filesystem::path db_file)
		: file_name_(std::move(db_file)) 
	{
		if (!std::filesystem::exists(file_name_)) {
			std::ofstream out(file_name_, std::ios::binary);
			out.close();
		}

		db_io_.open(file_name_, std::ios::binary | std::ios::in | std::ios::out);

		if (!db_io_.is_open()) {
			throw std::runtime_error("Failed to open db file");
		}

		next_page_id_ = static_cast<page_id_t>(GetFileSize() / PAGE_SIZE);

	}

	void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
		std::scoped_lock<std::mutex> lock(latch_);

		db_io_.clear();

		size_t offset = static_cast<size_t> (page_id) * PAGE_SIZE;
		db_io_.seekp(offset);
		db_io_.write(page_data, PAGE_SIZE);

		// Flush immediately to ensure durability (Optional but safer)
		db_io_.flush();
	}

	void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
		std::scoped_lock<std::mutex> lock(latch_);

		size_t offset = static_cast<size_t> (page_id) * PAGE_SIZE;

		db_io_.clear();

		db_io_.seekg(offset);
		db_io_.read(page_data, PAGE_SIZE);

		if (db_io_.bad()) {
			std::fill(page_data, page_data + PAGE_SIZE, 0);
			std::cerr << "I/O error whiloe reading" << std::endl;
		}

		int32_t cnt = db_io_.gcount();
		if (cnt < PAGE_SIZE) {
			std::fill(page_data, page_data + PAGE_SIZE, 0);
			std::cerr << "Read partial page: " << cnt << std::endl;
		}

	}

	page_id_t DiskManager::AllocatePage() {
		std::scoped_lock<std::mutex> lock(latch_);
		if (!free_list_.empty()) {
			page_id_t id = free_list_.top();
			free_list_.pop();
			return id;
		}

		return next_page_id_++;
	}

	void DiskManager::DeallocatePage(page_id_t page_id) {
		std::scoped_lock<std::mutex> lock(latch_);
		free_list_.push(page_id);
	}




} // namespace tetodb
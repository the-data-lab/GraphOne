
/*
 * Copyright 2016 The George Washington University
 * Written by Pradeep Kumar 
 * Directed by Prof. Howie Huang
 *
 * https://www.seas.gwu.edu/~howie/
 * Contact: iheartgraph@gmail.com
 *
 * 
 * Please cite the following paper:
 * 
 * Pradeep Kumar and H. Howie Huang. 2016. G-Store: High-Performance Graph Store for Trillion-Edge Processing. In Proceedings of the International Conference for High Performance Computing, Networking, Storage and Analysis (SC '16).
 
 *
 * This file is part of G-Store.
 *
 * G-Store is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * G-Store is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with G-Store.  If not, see <http://www.gnu.org/licenses/>.
 */




// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details

#ifndef BITMAP_H_
#define BITMAP_H_

#include <algorithm>



/*
GAP Benchmark Suite
Class:  Bitmap
Author: Scott Beamer

Parallel bitmap that is thread-safe
 - Can set bits in parallel (set_bit_atomic) unlike std::vector<bool>
*/

const uint64_t bits_per_word = 64;

inline uint64_t word_offset(size_t n) { return n/bits_per_word; }
inline uint64_t bit_offset(size_t n) { return n & (bits_per_word-1); }

class Bitmap {
 public:
    inline Bitmap() {
        start_ = 0;
        end_ = 0;
    }
    inline Bitmap(size_t size) {
        init(size);
    }
    inline void init(size_t size) {
        uint64_t num_words = (size + bits_per_word - 1) / bits_per_word;
        start_ = (uint64_t*)calloc(sizeof(uint64_t), num_words);
        end_ = start_ + num_words;

        //start_ = (uint64_t*)mmap(NULL, sizeof(uint64_t)*num_words, 
        //                   PROT_READ|PROT_WRITE,
        //                   MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0 , 0);
        //if (MAP_FAILED == start_)
    }

  inline size_t get_size() {
      return end_ - start_;
  }
  inline uint64_t* get_start() {
      return start_;
  }
  inline ~Bitmap() {
      //if (-1 == munmap(start_, sizeof(uint64_t)* (end_ - start_))) {
      //}
      free(start_);
  }

  inline void reset() {
    std::fill(start_, end_, 0);
  }
  
  inline void reset_bit(size_t pos) {
    start_[word_offset(pos)] &= ~((uint64_t) 1l << bit_offset(pos));
  }
  
  inline void set() {
    std::fill(start_, end_, -1L);
  }

  inline void set_bit(size_t pos) {
    start_[word_offset(pos)] |= ((uint64_t) 1l << bit_offset(pos));
  }

  inline void set_bit_atomic(size_t pos) {
    uint64_t old_val, new_val;
    do {
      old_val = start_[word_offset(pos)];
      new_val = old_val | ((uint64_t) 1l << bit_offset(pos));
    } while (!__sync_bool_compare_and_swap(start_ + word_offset(pos), old_val, new_val));
  }
  

  inline bool get_bit(size_t pos) const {
    return (start_[word_offset(pos)] >> bit_offset(pos)) & 1l;
  }

  inline void swap(Bitmap* other) {
    std::swap(start_, other->start_);
    std::swap(end_, other->end_);
  }

 private:
  uint64_t *start_;
  uint64_t *end_;
};

#endif  // BITMAP_H_

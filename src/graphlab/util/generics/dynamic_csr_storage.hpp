/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */
#ifndef GRAPHLAB_DYNAMIC_CSR_STORAGE
#define GRAPHLAB_DYNAMIC_CSR_STORAGE

#include <iostream>
#include <vector>
#include <algorithm>

#include <graphlab/util/generics/counting_sort.hpp>
#include <graphlab/util/generics/block_linked_list.hpp>

#include <graphlab/serialization/iarchive.hpp>
#include <graphlab/serialization/oarchive.hpp>

#include <boost/iterator/permutation_iterator.hpp>

namespace graphlab {
  /**
   * A compact key-value(s) data structure using Compressed Sparse Row format.
   * The key has type size_t and can be assolicated with multiple values of valuetype.
   *
   * The core operation of is querying the list of values associated with the query key
   * and returns the begin and end iterators via <code>begin(id)</code> and <code>end(id)</code>.
   *
   * Also, this class supports insert (and batch insert) values associated with any key. 
   */
  template<typename valuetype, typename sizetype=size_t, 
           uint32_t blocksize=(4096-20)/(4*sizeof(valuetype))> // the block size makes the block fit in a memory page
  class dynamic_csr_storage {
   public:
     typedef block_linked_list<valuetype, blocksize> block_linked_list_t;
     typedef typename block_linked_list_t::iterator iterator;
     typedef typename block_linked_list_t::const_iterator const_iterator;
     typedef typename block_linked_list_t::blocktype blocktype;
     typedef valuetype value_type;

   public:
     dynamic_csr_storage() { }

     /**
      * Create the storage with given keys and values. The id_vec and value_vec must  
      * have the same size.
      */
     template<typename idtype>
     dynamic_csr_storage(const std::vector<idtype>& id_vec,
                         const std::vector<valuetype>& value_vec) {
        init(id_vec, value_vec);
     }


     /**
      * Wrap the index vector and value vector into csr_storage.
      * Check the property of the input vector. 
      * Clean up the input on finish. 
      */
     void wrap(std::vector<sizetype>& valueptr_vec,
               std::vector<valuetype>& value_vec) {

       for (ssize_t i = 1; i < (ssize_t)valueptr_vec.size(); ++i) {
         ASSERT_LE(valueptr_vec[i-1], valueptr_vec[i]);
         ASSERT_LT(valueptr_vec[i], value_vec.size());
       }

       values.assign(value_vec.begin(), value_vec.end());
       sizevec2ptrvec(valueptr_vec, value_ptrs);

       std::vector<value_type>().swap(value_vec);
       std::vector<sizetype>().swap(valueptr_vec);
     }

     /// Number of keys in the storage.
     inline size_t num_keys() const { return value_ptrs.size(); }

     /// Number of values in the storage.
     inline size_t num_values() const { return values.size(); }

     /// Return iterator to the begining value with key == id 
     inline iterator begin(size_t id) {
       return id < num_keys() ? value_ptrs[id] : values.end();
     } 

     /// Return iterator to the ending+1 value with key == id 
     inline iterator end(size_t id) {
       return (id+1) < num_keys() ? value_ptrs[id+1] : values.end();
     }

     /// Return iterator to the begining value with key == id 
     inline const_iterator begin(size_t id) const {
       return id < num_keys() ? value_ptrs[id] : values.end();
     } 

     /// Return iterator to the ending+1 value with key == id 
     inline const_iterator end(size_t id) const {
       return (id+1) < num_keys() ? value_ptrs[id+1] : values.end();
     }
     
     ////////////////////////// Deletion API ////////////////////////
     /// Delete values for a given key.
     template <typename idtype>
     void delete_(const idtype& key, int target_vid) {
       // iterator to the insertion position
       std::cout << "[csr storage] delete key: " << key << " with target vid: " << target_vid << std::endl;
       iterator iter_end = end(key);
       iterator iter_begin = begin(key);
       if (iter_begin.get_blockptr() == NULL) {
           std::cout << "\treturn" << std::endl;
         return;
       }
       if (target_vid != -1) {
           iterator ii = iter_begin;
           while (ii != iter_end) {
               std::pair<uint32_t, uint32_t> pp = *ii;
               uint32_t vvid = pp.first;
               std::cout << "vvid" << vvid << std::endl;
               if (vvid == (unsigned int)target_vid) {
                   std::cout << "\t an edge is found, now ii offset = " << ii.get_offset() << std::endl;
                   iterator next = iterator(ii.get_blockptr(), ii.get_offset());
                   next ++;
                   
                   std::cout << "\t next offset = " << next.get_offset() << std::endl;
                   std::pair<iterator, iterator> iter_pair = values.delete_(ii, next);
                   //value_ptrs[key] = iter_pair.first;
                   // if it's the last in block list and there's no edges left, delete the key.
                   std::cout << "\t after delete, the first is: " << iter_pair.first.get_blockptr() << ":" << iter_pair.first.get_offset() << std::endl;
                   if (iter_pair.second.get_blockptr() == NULL) {
                       std::cout << "\t iter second is null" << std::endl;
                     if (ii == iter_begin) {
                       value_ptrs.erase(value_ptrs.begin() + key);
                       break;
                     }
                   }
                   iterator first = iter_pair.first;
                   iterator second = iter_pair.second;
                   if (second.get_blockptr() == first.get_blockptr()) {
                       std::cout << "after delete, the first and second is in the same block\n";
                     if (first.get_offset() == first.get_blockptr()->size() - 1) {
                         std::cout << "the last item is deleted, ii should inc\n";
                       ii ++;
                     }
                     if (iter_end.get_blockptr() == ii.get_blockptr()) {
                         std::cout << "the next iter should be move forwarded, the origin offset is : " << iter_end.get_offset() << "\n";
                       iter_end = iterator(iter_end.get_blockptr(), iter_end.get_offset() - 1);
                       value_ptrs[key + 1] = iter_end;
                       for (uint32_t kk = key + 2; kk < value_ptrs.size(); kk ++) {
                         if (value_ptrs[kk].get_blockptr() == ii.get_blockptr()) {
                           iterator ti = iterator(value_ptrs[kk].get_blockptr(), value_ptrs[kk].get_offset() - 1);
                           value_ptrs[kk] = ti;
                         }
                       }
                     }
                     continue;
                   }
               }
               ii ++;
           }
           return;
       }
       std::pair<iterator, iterator> iter_pair = values.delete_(iter_begin, iter_end);
       value_ptrs[key] = iter_pair.first;
       if (iter_pair.second.get_blockptr() == NULL) {
         value_ptrs.erase(value_ptrs.begin() + key);
       }
       if ((int)(key + 1) < (int)value_ptrs.size()) {
           std::cout << "the end iter is changed" << std::endl;
         value_ptrs[key + 1] = iter_pair.second;
       ASSERT_EQ(value_ptrs[key+1].get_blockptr(), iter_pair.second.get_blockptr());
       ASSERT_EQ(value_ptrs[key+1].get_offset(), iter_pair.second.get_offset());
       }
       
     }

     void print_block() {
       std::cout << "---------Print block list begin-------\n";
       std::cout << "[Value_ptrs]\n";
       for (uint32_t i = 0; i < value_ptrs.size(); i++) {
           std::cout << value_ptrs[i].get_blockptr() << " " << value_ptrs[i].get_offset() << std::endl;
       }
       std::cout << "[Blocks:]\n";
       blocktype* bp = values.begin().get_blockptr();
       while (bp != NULL) {
         std::cout << bp << " " << bp->size();
         bp = bp->next();
       }
       std::cout << "--------Print block list end-------\n";
     }


     ////////////////////////// Insertion API ////////////////////////
     /// Insert a new value to a given key
     template <typename idtype>
     void insert (const idtype& key, const valuetype& value) {
       insert(key, &value, (&value + 1));
     }

     /// Insert a range of values to a given key
     template <typename idtype, typename InputIterator>
     void insert (const idtype& key, InputIterator first, InputIterator last) {
       if (last-first == 0) {
         return;
       }
       // iterator to the insertion position
       iterator ins_iter = end(key);

       // begin_ins_iter and end_ins_iterator point to 
       // defines the range of the new inserted element.
       std::pair<iterator,iterator> iter_pair =  values.insert(ins_iter, first, last);
       iterator begin_ins_iter = iter_pair.first;
       iterator end_ins_iter =  iter_pair.second;

       // add blocks for new key
       while (key >= num_keys()) {
         value_ptrs.push_back(begin_ins_iter);
       }

       // Update pointers. 
       // value_ptrs[key] = begin_ins_iter;
       // ASSERT_TRUE(begin_ins_iter == ins_iter);
       
       // Update pointers to the right of ins_iter. 
       // Base case: the pointer of ins_iter is mapped to end_ins_iter. 
       uint32_t oldoffset =  ins_iter.get_offset();
       iterator newiter =  end_ins_iter;
       for (size_t scan = key+1; scan < num_keys(); ++scan) {
         if (value_ptrs[scan].get_blockptr() == ins_iter.get_blockptr()) {
           while (oldoffset != value_ptrs[scan].get_offset()) {
             ++oldoffset;
             ++newiter;
           }
           value_ptrs[scan] = newiter;
         } else {
           break;
         }
       }
     }

     /// Repack the values in parallel
     void repack() {
       // values.print(std::cerr);
#ifdef _OPENMP
#pragma omp parallel for
#endif
       for (ssize_t i = 0; i < (ssize_t)num_keys(); ++i) {
           values.repack(begin(i), end(i));
       }
     }

     /////////////////////////// I/O API ////////////////////////
     /// Debug print out the content of the storage;
     void print(std::ostream& out) const {
       for (size_t i = 0; i < num_keys(); ++i)  {
         const_iterator iter = begin(i);
          out << i << ": ";
          // out << "begin: " << iter.get_blockptr() << " " << iter.get_offset() << std::endl;
          // out << "end: " << end(i).get_blockptr() << " " << end(i).get_offset() << std::endl;
         while (iter != end(i)) {
           out << *iter <<  " ";
           ++iter;
         }
         out << std::endl;
       }
     }

     void swap(dynamic_csr_storage<valuetype, sizetype>& other) {
       value_ptrs.swap(other.value_ptrs);
       values.swap(other.values);
     }

     void clear() {
       std::vector<iterator>().swap(value_ptrs);
       values.clear();
     }

     void load(iarchive& iarc) { 
       clear();
       std::vector<sizetype> valueptr_vec;
       std::vector<valuetype> all_values;
       iarc >> valueptr_vec >> all_values;

       wrap(valueptr_vec, all_values);
     }

     void save(oarchive& oarc) const { 
       std::vector<sizetype> valueptr_vec(num_keys(), 0);
       for (size_t i = 1;i < num_keys(); ++i) {
         const_iterator begin_iter = begin(i - 1);
         const_iterator end_iter = end(i - 1);
         sizetype length = begin_iter.pdistance_to(end_iter);
         valueptr_vec[i] = valueptr_vec[i - 1] + length;
       }

       std::vector<valuetype> out;
       std::copy(values.begin(), values.end(), std::inserter(out, out.end()));

       oarc << valueptr_vec << out;
     }

     ////////////////////// Internal APIs /////////////////
   public:
     /**
      * \internal
      */
     const std::vector<iterator>& get_index() { return value_ptrs; }
     const block_linked_list_t& get_values() { return values; }

     size_t estimate_sizeof() const {
       return sizeof(value_ptrs) + sizeof(values) + sizeof(sizetype)*value_ptrs.size() + sizeof(valuetype) * values.size();
     }

     void meminfo(std::ostream& out) {
       out << "num values: " <<  (float)num_values()
                 << "\n num blocks: " << values.num_blocks()
                 << "\n block size: " << blocksize
                 << std::endl;
       out << "utilization: " <<  (float)num_values() / (values.num_blocks() * blocksize) << std::endl;
     }

     ///////////////////// Helper Functions /////////////
   private:
     /**
      * Initialize the internal member with input key_vec and value_vec.
      * value_vec will be compactly wrapped into a block_linked_list
      * and key_vec will be converted into an array of iterators (pointers)
      * to the values in the block_linked_list.
      */
     template<typename idtype>
     void init(const std::vector<idtype>& id_vec,
               const std::vector<valuetype>& value_vec) {
      ASSERT_EQ(id_vec.size(), value_vec.size());
      std::vector<sizetype> permute_index;

      // Build index for id -> value 
      // Prefix of the counting array equals to the begin index for each id
      std::vector<sizetype> prefix;
      counting_sort(id_vec, permute_index, &prefix);

      // Fill in the value vector
      typedef boost::permutation_iterator<
               typename std::vector<valuetype>::const_iterator,
               typename std::vector<sizetype>::const_iterator> permute_iterator;
      permute_iterator _begin = boost::make_permutation_iterator(value_vec.begin(), permute_index.begin());
      permute_iterator _end = boost::make_permutation_iterator(value_vec.end(), permute_index.end());
      values.assign(_begin, _end);

      // Fill in the key vector
      sizevec2ptrvec(prefix, value_ptrs);

      // Build the index pointers 
#ifdef DEBUG_CSR
      for (size_t i = 0; i < permute_index.size(); ++i)
        std::cerr << permute_index[i] << " ";
      std::cerr << std::endl;

      for (size_t i = 0; i < value_ptrs.size(); ++i)
        std::cerr << prefix[i] << " ";
      std::cerr << std::endl;

      for (permute_iterator it = _begin; it != _end; ++it) {
        std::cerr << *it << " ";
      }
      std::cerr << std::endl;

      for (size_t i = 0; i < num_keys(); ++i) {
        std::cerr << i << ": ";
        iterator it = begin(i);
        while (it != end(i)) {
          std::cerr << *it << " "; 
          ++it;
        }
        std::cerr << std::endl;
      }
      std::cerr << std::endl;
#endif
     }

     // Convert integer pointers into block_linked_list::value_iterator
     // Assuming all blocks are fully packed.
     void sizevec2ptrvec (const std::vector<sizetype>& ptrs,
                          std::vector<iterator>& out) {
       ASSERT_EQ(out.size(), 0);
       out.reserve(ptrs.size());

       // for efficiency, we advance pointers based on the previous value
       // because block_linked_list is mostly forward_traversal.
       iterator it = values.begin();
       sizetype prev = 0;
       for (size_t i = 0; i < ptrs.size(); ++i) {
         sizetype cur = ptrs[i];
         it += (cur-prev);
         out.push_back(it);
         prev = cur; 
       }
     }

   private:
     std::vector<iterator> value_ptrs;
     block_linked_list_t values;
  }; // end of class
} // end of graphlab 
#endif

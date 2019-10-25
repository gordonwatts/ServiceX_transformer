# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# import ROOT
# import numpy as np
import uproot
import awkward


def _parse_column_name(attr):
    attr_parts = attr.split('.')
    tree_name = attr_parts[0]
    branch_name = '.'.join(attr_parts[1:])
    return tree_name, branch_name

class NanoAODEvents:

    def __init__(self, file_path, attr_name_list, chunk_size):

        self.file_path = file_path
        self.file_in = uproot.open(file_path)
        self.tree_map = {}

        for col in attr_name_list:
            (tree_name, branch_name) = _parse_column_name(col)
            if tree_name not in self.tree_map:
                self.tree_map[tree_name] = [branch_name]
            else:
                self.tree_map[tree_name].append(branch_name)

        self.attr_name_list = attr_name_list
        self.sample_tree = list(self.tree_map.keys())[0]
        self.chunk_size = chunk_size

    def get_entry_count(self):
        sample_tree = list(self.tree_map.keys())[0]
        return self.file_in[sample_tree].numentries

    def iterate(self, event_limit=None):
        iterators = []
        for tree in self.tree_map.keys():
            iterators.append(self.file_in[tree].iterate(self.tree_map[tree],
                                                        entrysteps=self.chunk_size))
        for result in iterators[0]:
            for remaining in iterators[1:]:
                sub_result = remaining.next()
                result.update(sub_result)
            print result
            yield result

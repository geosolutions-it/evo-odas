#!/usr/bin/env python

def wrap_keys_among_brackets(dict):
    wrappedDict = {}
    for key in dict.keys():
        wrappedKey = '"' + key + '"'
        wrappedDict[wrappedKey] = dict[key]
    return wrappedDict

def join(dict1, dict2):
    all_metadata = dict1.copy()
    all_metadata.update(dict2)
    return all_metadata

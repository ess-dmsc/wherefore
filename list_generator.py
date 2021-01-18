from typing import List
from math import ceil, floor


def generate_list(items: List, list_rows: int, selection: int):
    return_list = []
    arrows_at_end = False
    arrows_at_start = False
    lower_copy_index = int(selection - floor(list_rows/2))
    if lower_copy_index < 0 or len(items) <= list_rows:
        lower_copy_index = 0
    upper_copy_index = lower_copy_index + list_rows
    if upper_copy_index > len(items):
        upper_copy_index = len(items)
    if upper_copy_index - lower_copy_index < list_rows:
        lower_copy_index = upper_copy_index - list_rows
    if lower_copy_index < 0:
        lower_copy_index = 0
    if len(items) > list_rows and upper_copy_index < len(items):
        arrows_at_end = True
    if lower_copy_index > 0:
        arrows_at_start = True
    for i in range(lower_copy_index, upper_copy_index):
        return_list.append(items[i])
    if arrows_at_end:
        return_list[-1] = "↓"
    if arrows_at_start:
        return_list[0] = "↑"
    return return_list, selection - lower_copy_index


import curses
from list_generator import generate_list
from typing import Union, Dict, List, Optional
from wherefore.DataSource import DataSource
from datetime import datetime, timezone
from wherefore.KafkaMessageTracker import HighLowOffset

DEBUG = False
DEFAULT_LIST_ROWS = 9
DEFAULT_LIST_WIDTH = 80


def time_to_str(timestamp: Optional[datetime]):
    if timestamp is None:
        return "n/a"
    return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f %Z")


def get_time_diff(time1, time2):
    if time1 is None or time2 is None:
        return "n/a"
    return f"{(time1 - time2).total_seconds():8.2f}"


def draw_full_line(window, row, text, invert=False):
    height, width = window.getmaxyx()
    print_width = len(text)
    if print_width == width:
        pass
    elif print_width > width:
        print_width = width
        text = text[0: print_width - 3] + "..."
    draw_function = window.insstr
    if len(text) < width:
        draw_function = window.addstr
    if invert:
        draw_function(row, 0, text[0:print_width], curses.A_REVERSE)
    else:
        draw_function(row, 0, text[0:print_width])


class CursesRenderer:
    def __init__(self, topic_name, partition):
        self.topic_name = topic_name
        self.partition = partition
        self.screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        self.screen.keypad(1)
        self.screen.timeout(0)
        self.selected_item = 0
        self.current_offsets = HighLowOffset(-1, -1)
        self.known_sources: List[DataSource] = []

    def __del__(self):
        if not DEBUG:
            curses.nocbreak()
            curses.curs_set(1)
            self.screen.keypad(0)
            curses.echo()
            curses.endwin()

    def set_data(self, data: Union[Dict, List]):
        if type(data) is dict:
            self.known_sources = list(data.values())
        elif type(data) is list:
            self.known_sources = data
        else:
            raise RuntimeError("Can not set data of type: " + str(type(data)))

    def set_partition_offsets(self, current_offsets: HighLowOffset):
        self.current_offsets = current_offsets

    def draw_sources_list(self):
        height, width = self.screen.getmaxyx()
        used_width = DEFAULT_LIST_WIDTH
        prototype_line = "{:^6.6s}| {:40.40s}| {:30.30s}"
        draw_full_line(self.screen, 2, prototype_line.format("Id", "Name", "Last timestamp"))
        self.screen.addstr(3, 0, "-" * used_width)
        if width < used_width:
            used_width = width
        used_height = DEFAULT_LIST_ROWS
        if used_height + 3 > height:
            used_height = height - 3
        sources_win = curses.newwin(used_height, used_width, 4, 0)
        win_height, win_width = sources_win.getmaxyx()
        available_rows = win_height
        sources_to_list, selection_row = generate_list(self.known_sources, available_rows, self.selected_item)
        for i, item in enumerate(sources_to_list):
            selected = False
            if i == selection_row:
                selected = True
            if type(item) is str:
                draw_full_line(sources_win, i, "  " + item*3)
            else:
                draw_full_line(sources_win, i, prototype_line.format(item.source_type, item.source_name, time_to_str(item.last_timestamp)), selected)
        sources_win.refresh()

    def draw_selected_source_info(self):
        height, width = self.screen.getmaxyx()
        if self.selected_item < 0:
            return
        current_source = self.known_sources[self.selected_item]
        current_message = current_source.last_message
        now = datetime.now(tz=timezone.utc)
        info_rows = []
        timestamp_format = "{:20.20s} | {:30.30s} | {:10.10s}"

        first_ts_string = time_to_str(current_source.first_timestamp)
        info_rows.append(f"First offset: {current_source.first_offset:>12d}    First timestamp: {first_ts_string}")
        info_rows.append(f"Current offset: {current_message.offset:>10d}    Consumption rate: {current_source.processed_per_second:8.3f}/s")
        info_rows.append(f"Current msg. size: {current_message.size:>7d}    Message rate: {current_source.messages_per_second:12.3f}/s")
        info_rows.append(
            f"Received messages: {current_source.processed_messages:7d}    Data rate: {current_source.bytes_per_second:11.0f} bytes/s")
        info_rows.append(f"Data: {current_message.data}")
        info_rows.append(timestamp_format.format("Type", "Timestamp", "Age (s)"))
        info_rows.append("-"*80)
        info_rows.append(timestamp_format.format("Receive time", time_to_str(current_message.local_timestamp), get_time_diff(now, current_message.local_timestamp)))
        info_rows.append(timestamp_format.format("Message time", time_to_str(current_message.timestamp),
                                                 get_time_diff(now, current_message.timestamp)))
        info_rows.append(timestamp_format.format("Kafka time", time_to_str(current_message.kafka_timestamp),
                                                 get_time_diff(now, current_message.kafka_timestamp)))
        if height > DEFAULT_LIST_ROWS + 3:
            self.screen.addstr(13, 0, "-" * DEFAULT_LIST_WIDTH)
        remaining_height = height - 14
        if remaining_height <= 0:
            return
        source_info_win = curses.newwin(remaining_height, width, 14, 0)
        for i in range(remaining_height):
            if i == len(info_rows):
                break
            draw_full_line(source_info_win, i, info_rows[i])
        source_info_win.refresh()

    def handle_key_press(self):
        key_pressed = self.screen.getch()
        if key_pressed == curses.KEY_DOWN:
            self.selected_item += 1
        elif key_pressed == curses.KEY_UP:
            self.selected_item -= 1
        if self.selected_item < 0:
            self.selected_item = 0
        if self.selected_item >= len(self.known_sources):
            self.selected_item = len(self.known_sources) - 1

    def draw(self):
        self.handle_key_press()
        height, width = self.screen.getmaxyx()
        header_line = f"Topic: {self.topic_name}, Partition#: {self.partition}"
        header_line2 = f"Low offset: {self.current_offsets.low:10d}  High offset: {self.current_offsets.high:10d}  Lag: {self.current_offsets.lag:10d}"
        draw_full_line(self.screen, 0, header_line)
        draw_full_line(self.screen, 1, header_line2)
        self.draw_sources_list()
        self.draw_selected_source_info()
        self.screen.refresh()


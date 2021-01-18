import curses
from datetime import datetime
from math import ceil, floor
from list_generator import generate_list

DEBUG = True
DEFAULT_LIST_ROWS = 10
DEFAULT_LIST_WIDTH = 80


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
    def __init__(self):
        self.screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.screen.keypad(1)
        self.screen.timeout(0)
        self.selected_item = 0
        self.known_sources = []
        for i in range(15):
            self.known_sources.append(["abcd", f"Name {i}", str(datetime.now())])

    def __del__(self):
        if not DEBUG:
            curses.nocbreak()
            self.screen.keypad(0)
            curses.echo()
            curses.endwin()

    def draw_sources_list(self):
        height, width = self.screen.getmaxyx()
        used_width = DEFAULT_LIST_WIDTH
        prototype_line = "{:^6.6s}| {:44.44s}| {:26.26s}"
        draw_full_line(self.screen, 1, prototype_line.format("Id", " Name", " Last timestamp"))
        self.screen.addstr(2, 0, "-" * used_width)
        if width < used_width:
            used_width = width
        used_height = DEFAULT_LIST_ROWS
        if used_height + 3 > height:
            used_height = height - 3
        sources_win = curses.newwin(used_height, used_width, 3, 0)
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
                draw_full_line(sources_win, i, prototype_line.format(item[0], item[1], item[2]), selected)
        sources_win.refresh()

    def draw_selected_source_info(self):
        height, width = self.screen.getmaxyx()
        if height > DEFAULT_LIST_ROWS + 3:
            self.screen.addstr(13, 0, "-" * DEFAULT_LIST_WIDTH)
        remaining_height = height - 14
        if remaining_height <= 0:
            return
        source_info_win = curses.newwin(remaining_height, width, 14, 0)
        for i in range(remaining_height):
            draw_full_line(source_info_win, i, f"Line number {i + 1}")
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
        header_line = f"Min. offset: 12432542    Max offset: 456786476    Msg/s: 12.5"
        draw_full_line(self.screen, 0, header_line)
        self.draw_sources_list()
        self.draw_selected_source_info()
        self.screen.refresh()




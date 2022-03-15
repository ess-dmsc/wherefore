from list_generator import generate_list
import pytest


def test_fewer_items_than_rows():
    nr_of_rows = 5
    items = list(range(3))
    for i in items:
        rows, selected_row = generate_list(items, nr_of_rows, i)
        assert rows == items
        assert selected_row == i


def test_empty_list():
    rows, selected_row = generate_list([], 10, -1)
    assert rows == []
    assert selected_row == -1


def test_same_number_of_rows_and_items():
    nr_of_rows = 5
    items = list(range(5))
    for i in items:
        rows, selected_row = generate_list(items, nr_of_rows, i)
        assert rows == items
        assert selected_row == i


@pytest.mark.parametrize("selection", [0, 1, 2])
def test_one_more_item_sel_012(selection):
    nr_of_rows = 5
    items = list(range(6))
    rows, selected_row = generate_list(items, nr_of_rows, selection)
    assert rows == [0, 1, 2, 3, "↓"]
    assert selected_row == selection


@pytest.mark.parametrize("selection", [3, 4, 5])
def test_one_more_item_sel_345(selection):
    nr_of_rows = 5
    items = list(range(6))
    rows, selected_row = generate_list(items, nr_of_rows, selection)
    assert rows == ["↑", 2, 3, 4, 5]
    assert selected_row == selection - 1


@pytest.mark.parametrize("selection", [0, 1, 2])
def test_two_more_items_sel_012(selection):
    nr_of_rows = 5
    items = list(range(7))
    rows, selected_row = generate_list(items, nr_of_rows, selection)
    assert rows == [0, 1, 2, 3, "↓"]
    assert selected_row == selection


def test_two_more_items_sel_3():
    nr_of_rows = 5
    items = list(range(7))
    rows, selected_row = generate_list(items, nr_of_rows, 3)
    assert rows == ["↑", 2, 3, 4, "↓"]
    assert selected_row == 3 - 1


@pytest.mark.parametrize("selection", [4, 5, 6])
def test_two_more_items_sel_456(selection):
    nr_of_rows = 5
    items = list(range(7))
    rows, selected_row = generate_list(items, nr_of_rows, selection)
    assert rows == [
        "↑",
        3,
        4,
        5,
        6,
    ]
    assert selected_row == selection - 2

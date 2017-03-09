from chunk import merge, invert, chunkize, size, inorder, merge_leftovers


def test_trivial():
  assert size((0, 2)) == 3
  assert inorder((0,1), (2,3)) == True
  assert inorder((0,1), (5,6)) == False
  assert chunkize(10, 3) == [(0, 2), (3, 5), (6, 8), (9, 9)]


def test_merge_leftovers():
  assert merge_leftovers(chunkize(10, 3), maxsize=4) == [(0, 2), (3, 5), (6, 9)]


def test_merge():
  assert merge([(0,1), (2,3), (5,6)]) == [(0, 3), (5, 6)]
  assert merge([(0,1), (5,6), (7,8)]) == [(0, 1), (5, 8)]
  assert merge([(0,1), (3,4), (6,7)]) == [(0,1), (3,4), (6,7)]


def test_invert():
  assert invert(10, [(0,3)]) == [(4,9)]
  assert invert(5, [(2,3)]) == [(0,1), (4,4)]

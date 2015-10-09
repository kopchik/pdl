#!/usr/bin/env python3

def chunkize(size, chunksize):
  l = []
  start, stop = 0, min(size, chunksize)-1
  while True:
    chunk = (start, stop)
    l.append(chunk)
    if stop == size-1:
      break
    start = stop + 1
    stop  = min(size-1, stop+chunksize)
  return l


def inorder(chunk1, chunk2):
  return chunk1[1] + 1 == chunk2[0]


def merge2(chunk1, chunk2):
  assert inorder(chunk1, chunk2)
  return chunk1[0], chunk2[1]

def merge(l):
  if len(l) == 1:
    return l
  result = []
  cur = l[0]
  for nxt in l[1:]:
    if inorder(cur, nxt):
      cur = merge2(cur, nxt)
    else:
      result.append(cur)
      cur = nxt
  print(result, cur)
  if result[-1] != cur:
    result.append(cur)
  return result


def size(chunk):
  return chunk[1] - chunk[0] + 1


def merge_leftovers(chunks, maxsize):
  result = []
  for c1, c2 in zip(chunks[::2], chunks[1::2]):
    if inorder(c1, c2) and size(c1) + size(c2) <= maxsize:
      result.append(merge2(c1,c2))
    else:
      result.append(c1)
      result.append(c2)
  return result


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


if __name__ == '__main__':
  chunks = chunkize(10, 3)
  print(chunks)
  test = [(0,1), (2,3), (4,5), (6,7)]
  print(test)
  print(merge_leftovers(chunkize(10, 3), 4))


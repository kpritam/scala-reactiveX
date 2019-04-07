

val sorted = List(5, 3, 2, 1).sorted


def counter(start: Int) = start + 1

sorted
  .zipWithIndex
  .filter {
    case (f, s) ⇒ f - s == sorted.head - 0
  }.map(_._1)


import followers.model.Event

val x = List(
  Event.Follow(2, 1, 2),
  Event.Follow(1, 1, 3),
  Event.Follow(4, 1, 4)
)


var nr = 1

x.sortBy(_.sequenceNr).takeWhile { e ⇒
  println(e)
  if (e.sequenceNr == nr) {
    nr += 1
    true
  } else false
}

println(nr)
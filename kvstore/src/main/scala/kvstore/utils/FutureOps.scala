package kvstore.utils

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object FutureOps {

  implicit class MyRichFuture[T](f: Future[T]) {
    def mapAll[U](pf: PartialFunction[Try[T], U])(implicit ec: ExecutionContext): Future[U] = {
      val p = Promise[U]()
      f.onComplete(r => p.complete(Try(pf(r))))
      p.future
    }
  }

}

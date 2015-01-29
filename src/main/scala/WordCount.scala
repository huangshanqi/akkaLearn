import akka.actor.{ActorSystem, Props, ActorRef, Actor}
/**
 * User: Huangshanqi
 * Date: 2015/1/29
 * Time: 11:04
 */
case class ProcessStringMsg(str:String)
case class ProcessStringResult(words:Integer)
class StringCountActor extends Actor {
  override def receive: Receive = {
    case ProcessStringMsg(str) => {
      val words = str.split(" ").length
      sender() ! ProcessStringResult(words)
    }
    case _ => println("Error:message not recognized")
  }
}

case class startProcessFileMsg()
class wordCountActor(filename:String) extends Actor {

  private var running = false
  private var totalLines = 0
  private var linesProcessed = 0
  private var result = 0
  private var fileSender:Option[ActorRef] = None

  override def receive: Actor.Receive = {
    case startProcessFileMsg() => {
      if(running){
        println("Error:process already started")
      }else{
        running = true
        fileSender = Some(sender)
        import scala.io.Source._
        fromFile(filename).getLines().foreach {
          line => {
            context.actorOf(Props[StringCountActor],totalLines.toString) ! ProcessStringMsg(line)
            totalLines +=1
          }
        }
      }

    }

    case ProcessStringResult(words) => {
      result += words
      linesProcessed += 1
      if(linesProcessed == totalLines){
        fileSender.map( _ ! result)
      }
    }
    case _ => println("Error:message not recognized")
  }
}

object WordCount extends App{
  val start = System.currentTimeMillis()/1000
  println("start at " + start)


  import akka.util.Timeout
  import scala.concurrent.duration._
  import akka.pattern.ask
  import akka.dispatch.ExecutionContexts._
  implicit val ec = global
  val system = ActorSystem("System")
  val actor = system.actorOf(Props(new wordCountActor(System.getProperty("user.dir")+"/sources/tt.txt")))
  implicit val timeout = Timeout(25 seconds)
  val future = actor ? startProcessFileMsg()
  future.map { result =>
    println("Total number of words " + result)
    system.shutdown()

    val end = System.currentTimeMillis()/1000
    println("end at " + end +" ,total used "+ (end-start) + "s")
  }

  //  future.onComplete {
//    case Success(result) => {
//      println("total word====" + result)
//      system.shutdown()
//    }
//
//  }
//  future.onFailure {
//    case Failure(result) => {
//      println("failure message ===" + result)
//    }
//  }
}

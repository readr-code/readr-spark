import org.allenai.nlpstack.tokenize.defaultTokenizer
import org.allenai.nlpstack.postag.defaultPostagger
import org.allenai.nlpstack.parse.defaultDependencyParser

/* ... */

object Test {

  def main(args:Array[String]) = {
    val tokens = defaultTokenizer.tokenize(
      "I was wondering why the ball kept getting bigger and bigger, and then it hit me.")
    val postaggedTokens = defaultPostagger.postagTokenized(tokens)
    //val dependencyGraph = defaultDependencyParser.dependencyGraphPostagged(postaggedTokens)
    val ptp = new org.allenai.nlpstack.parse.PolytreeParser
    val dependencyGraph = ptp.dependencyGraphPostagged(postaggedTokens)
    
    println(dependencyGraph)
  }

}


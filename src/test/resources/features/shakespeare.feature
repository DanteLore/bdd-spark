Feature: shakespeare word count

  In order to know the amount of words in shakespeare's completed works
  As a thespian actor
  I want to be able to count the number of words in all of shakespeare's works

  Scenario: Counting the words in shakespeare
    Given a file called "shakespeare" containing
    """
    Romeo, Romeo wherefore art thou Romeo?
    """
    When I count the words
    Then the result is '6'

   Scenario: Counting the number of romeos in shakespeare
     Given a file called "shakespeare" containing
     """
     Romeo, Romeo wherefore art thou Romeo?
     Where is my Romeo?
     """
     When I count the romeos
     Then the result is '4'

   Scenario: Calculate the average number of romeos per line
     Given a file called "shakespeare" containing
     """
     Romeo, Romeo wherefore art thou Romeo?
     Where is my Romeo?
     """
     When I average the romeos
     Then the result is '2'

  Scenario: Average number of Romeos with file reader injection and map reduce
    Given a file called "shakespeare.txt" containing
     """
     Romeo, Romeo wherefore art thou Romeo?
     Where is my Romeo?
     """
    When I calculate the average "romeo" count per line in "shakespeare.txt"
    Then the result is '2'

  Scenario: Only count the spoken words
    Given a file called "shakespeare" containing
     """
     Juliet: Romeo, Romeo wherefore art thou Romeo?
     [Romeo ingests poison]
     [Juliet Wakes]
     Romeo: Woops
     Juliet: OMGIES
     [Juliet Also injests poison]
     """
    When I count spoken words
    Then the result is '8'

  Scenario: Only count the spoken words injecting a file reader
    Given a file called "shakespeare.txt" containing
     """
     Juliet: Romeo, Romeo wherefore art thou Romeo?
     [Romeo ingests poison]
     [Juliet Wakes]
     Romeo: Woops
     Juliet: OMGIES
     [Juliet Also ingests poison]
     """
    When I count spoken words Dan style from file "shakespeare.txt"
    Then the result is '8'

  Scenario: Count the most popular spoken words
    Given a file called "shakespeare.txt" containing
     """
     Juliet: Romeo, Romeo wherefore art thou Romeo?
     [Romeo ingests poison]
     [Juliet Wakes]
     Romeo: Woops
     Juliet: OMGIES
     [Juliet Also ingests poison]
     """
    When I count the spoken words in "shakespeare.txt" saving results to table "wordCounts"
    Then the data in temp table "wordCounts" is
      | word:String  | count:Int |
      | romeo        | 3         |
      | wherefore    | 1         |
      | art          | 1         |
      | thou         | 1         |
      | omgies       | 1         |
      | woops        | 1         |
Feature: Basic Spark

  In order to prove you can do simple BDD with spark
  As a developer
  I want some spark tests

  Scenario: Count some words with an RDD
    When I count the words in "the complete works of Shakespeare"
    Then the number of words is '5'

  Scenario: Do something with a table
    Given a table of house price data in a temp table "housePrices"
      | Price  | Postcode | HouseType |
      | 318000 | NN9 6LS  | D         |
      | 137000 | NN3 8HJ  | T         |
      | 180000 | NN12 8NN | S         |
      | 249000 | NN14 6TN | D         |
      | 137950 | NN8 3JA  | T         |
      | 211000 | NN14 2UY | D         |
      | 255000 | CV23 8XH | D         |
      | 127000 | NN8 1HT  | T         |
      | 1225000| AL3 4PA  | T         |
      | 351500 | WR5 2DA  | D         |
    When I execute SQL "select max(price) as price from housePrices"
    Then the query result is '1225000'

  Scenario Outline: Do lots of things with a table
    Given a table of house price data in a temp table "housePrices"
      | Price  | Postcode | HouseType |
      | 318000 | NN9 6LS  | D         |
      | 137000 | NN3 8HJ  | T         |
      | 180000 | NN12 8NN | S         |
      | 249000 | NN14 6TN | D         |
      | 137950 | NN8 3JA  | T         |
      | 211000 | NN14 2UY | D         |
      | 255000 | CV23 8XH | D         |
      | 127000 | NN8 1HT  | T         |
      | 1225000| AL3 4PA  | T         |
      | 351500 | WR5 2DA  | D         |
    When I execute SQL "<Sql>"
    Then the query result is '<Expected>'
  Examples:
    | Sql                                         | Expected    |
    | select max(price) as price from housePrices | 1225000     |
    | select min(price) as price from housePrices | 127000      |
    | select count(1) as cnt from housePrices     | 10          |

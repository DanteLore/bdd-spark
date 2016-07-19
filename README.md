# Behaviour Driven Development for Spark

Because your code does something very clever but your stakeholders don't have a clue how to read Scala!

```cucumber
Scenario: Read data from CSV files, join and save it to parquet
    Given a file called "housePrices.csv" containing
    """
      318000,NN9 6LS,D
      137000,NN3 8HJ,T
      180000,NN14 6TN,S
      249000,NN14 6TN,D
    """
    And a file called "postcodes.csv" containing
    """
      NN9 6LS,51.1,-1.2
      NN3 8HJ,51.2,-1.1
      NN14 6TN,51.3,-1.0
    """
    When I read the data from "housePrices.csv" and "postcodes.csv" join then save to parquet
    Then the parquet data written to "results.parquet" is
      | Price:Int  | Postcode:String | HouseType:String | Latitude:Double | Longitude:Double |
      | 318000     | NN9 6LS         | D                | 51.1            | -1.2             |
      | 137000     | NN3 8HJ         | T                | 51.2            | -1.1             |
      | 180000     | NN14 6TN        | S                | 51.3            | -1.0             |
      | 249000     | NN14 6TN        | D                | 51.3            | -1.0             |
```

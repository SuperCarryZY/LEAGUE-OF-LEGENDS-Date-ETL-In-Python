# Python-ETL-of-LEAGUE-OF-LEGENDS-Data
This project is to design ETL, and the data set is the ranking data of the League of Heroes. We extract data from csv and find ways to improve the winning rate of players from these data

![image](https://user-images.githubusercontent.com/121896846/220762906-2cf60d3c-d489-4c92-a996-e11b999ae395.png)

# About Dataset
League of Legends (LOL) - Ranking game 2020


# Data summary
## match_data4
108,000 row
gameId : game id
gameDuration : game time
participants : In-game information of 10 summoners who participated in the game

Link of google Drive: ttps://drive.google.com/file/d/17y9QIiNQip326Gt23SXYFNiGNNk-bX_S/view?usp=sharing


## CHAMPION
* vision : lol version
* name : champion name
* key : champion_id(other data foreign key)
* title : champion title
* blurb : champion explain
* etc
## ITEM
* item_id : item_id(other data foreign key)
* name : item name
* upper_item
* explain : item explain
* buy_price : item price
* sell_price : item sell price

# Purpose
1.The goal of this project is to create an ETL
process to extract data from semi-structured
data and download CSV from spark. The
purpose is to help players better analyze
game data.
2.he goal of this project is to create an ETL
process to extract data from semi-structured
data and download CSV from spark. The
purpose is to help players better analyze
game data.
2.From structured and semi-structured data,
find the most used items, highest win rate
champions,etc.


## MATCH RECORD
These two sets of data contain all the data of
the 10 players and are stored in match-record
as an array structure. So it's going to be very
difficult for us to extract this semi-structured
data in SQL.


![image](https://user-images.githubusercontent.com/121896846/220760579-3602d0b7-4514-4813-9b99-705af55d629a.png)
![image](https://user-images.githubusercontent.com/121896846/220760587-30ef575a-6eae-4413-898e-c81c0e137d50.png)

## ERD
![image](https://user-images.githubusercontent.com/121896846/220760639-fae66c76-de2f-4831-ab98-27e2e356f96d.png)


## Function in project
* Struct_type: PySpark provides from pyspark.sql.types import StructType class to define the structure of the
DataFrame.
*  sc.parallelize(): When spark parallelize method is applied on a Collection (with elements), a new distributed data
set is created with specified number of partitions and the elements of the collection are copied to the distributed
dataset
* StructField(): provides spark.sql.types.StructField class to define the column name(String), column type.
* Arraytype(): is used to define an array data type column on DataFrame
* distinct().collect(): distinct value in the data frame column
* lambda(): A lambda function is a small anonymous function
* explode():Returns a new row for each element in the given array


## Query the win rateof all champions played by each player:

                           +--------------------+-------------+-----------+-------------+------------------+
                           |                user|name_champion|won_matches|total_matches|          win_rate|
                           +--------------------+-------------+-----------+-------------+------------------+
                           |zpdpHmvHki76wDo0T...|       Kai'Sa|          1|            1|               1.0|
                           |zpdpHmvHki76wDo0T...| Miss Fortune|          1|            1|               1.0|
                           |zpdpHmvHki76wDo0T...|         Ashe|          1|            1|               1.0|
                           |zpdpHmvHki76wDo0T...|      Kalista|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|      Lee Sin|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|        Shaco|          2|            3|0.6666666666666666|
                           |zkYtjdZq7DuYLDNAt...|      Vel'Koz|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|       Maokai|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|     Vladimir|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|    Gangplank|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|      Taliyah|          1|            1|               1.0|
                           |zkYtjdZq7DuYLDNAt...|        Ivern|          2|            2|               1.0|
                           |zjYZ8Q5294hGgSJ1j...| Aurelion Sol|          1|            1|               1.0|
                           |zjYZ8Q5294hGgSJ1j...| Miss Fortune|          1|            1|               1.0|
                           |zhViuNtfWlfBpB4Tf...|         Lulu|          1|            1|               1.0|
                           |z_S77UAUcZ0hV1RLe...|        Talon|          1|            1|               1.0|
                           |zXZzJ3rOgLqEFlUzm...|         Sett|          1|            3|0.3333333333333333|
                           |zXZzJ3rOgLqEFlUzm...|        Yasuo|          1|            1|               1.0|
                           |zXZzJ3rOgLqEFlUzm...|     Pantheon|          4|            4|               1.0|
                           |zXZzJ3rOgLqEFlUzm...|     Kassadin|          1|            1|               1.0|
                           +--------------------+-------------+-----------+-------------+------------------+
                          
## Query the win rate of all champions played by each player:
                           +------+-------+-----------+---+------------+--------------------+--------------------+--------------------+----------+-----------+------------+----------+---------------+
                           |colum0|version|         id|key|        name|               title|               blurb|                tags|                              partype|info.attack|info.defense|info.magic|info.difficulty|
                           +------+-------+-----------+---+------------+--------------------+--------------------+--------------------+----------+-----------+---------                           ---+----------+---------------+
                           |     0| 10.6.1|     Aatrox|266|      Aatrox|    the Darkin Blade|Once honored defe...| ['Fighter', 'Tank']|Blood Well|          8|           4|         3|              4|
                           |     0| 10.6.1|       Ahri|103|        Ahri| the Nine-Tailed Fox|Innately connecte...|['Mage', 'Assassin']|      Mana|          3|           4|         8|              5|
                           |     0| 10.6.1|      Akali| 84|       Akali|  the Rogue Assassin|Abandoning the Ki...|        ['Assassin']|    Energy|          5|           3|         8|              7|
                           |     0| 10.6.1|    Alistar| 12|     Alistar|        the Minotaur|Always a mighty w...| ['Tank', 'Support']|      Mana|          6|           9|         5|              7|
                           |     0| 10.6.1|      Amumu| 32|       Amumu|       the Sad Mummy|Legend claims tha...|    ['Tank', 'Mage']|      Mana|          2|           6|         8|              3|
                           |     0| 10.6.1|     Anivia| 34|      Anivia|     the Cryophoenix|Anivia is a benev...| ['Mage', 'Support']|      Mana|          1|           4|        10|             10|
                           |     0| 10.6.1|      Annie|  1|       Annie|      the Dark Child|Dangerous, yet di...|            ['Mage']|      Mana|          2|           3|        10|              6|
                           |     0| 10.6.1|   Aphelios|523|    Aphelios|the Weapon of the...|Emerging from moo...|        ['Marksman']|      Mana|          6|           2|         1|             10|
                           |     0| 10.6.1|       Ashe| 22|        Ashe|    the Frost Archer|Iceborn warmother...|['Marksman', 'Sup...|      Mana|          7|           3|         2|              4|
                           |     0| 10.6.1|AurelionSol|136|Aurelion Sol|     The Star Forger|Aurelion Sol once...|            ['Mage']|      Mana|          2|           3|         8|              7|
                           |     0| 10.6.1|       Azir|268|        Azir|the Emperor of th...|Azir was a mortal...|['Mage', 'Marksman']|      Mana|          6|           3|         8|              9|
                           |     0| 10.6.1|       Bard|432|        Bard|the Wandering Car...|A traveler from b...| ['Support', 'Mage']|      Mana|          4|           4|         5|              9|
                           |     0| 10.6.1| Blitzcrank| 53|  Blitzcrank|the Great Steam G...|Blitzcrank is an ...| ['Tank', 'Fighter']|      Mana|          4|           8|         5|              4|
                           |     0| 10.6.1|      Brand| 63|       Brand|the Burning Venge...|Once a tribesman ...|            ['Mage']|      Mana|          2|           2|         9|              4|
                           |     0| 10.6.1|      Braum|201|       Braum|the Heart of the ...|Blessed with mass...| ['Support', 'Tank']|      Mana|          3|           9|         4|              3|
                           |     0| 10.6.1|    Caitlyn| 51|     Caitlyn|the Sheriff of Pi...|Renowned as its f...|        ['Marksman']|      Mana|          8|           2|         2|              6|
                           |     0| 10.6.1|    Camille|164|     Camille|    the Steel Shadow|Weaponized to ope...| ['Fighter', 'Tank']|      Mana|          8|           6|         3|              4|
                           |     0| 10.6.1| Cassiopeia| 69|  Cassiopeia|the Serpent's Emb...|Cassiopeia is a d...|            ['Mage']|      Mana|          2|           3|         9|             10|
                           |     0| 10.6.1|    Chogath| 31|    Cho'Gath|the Terror of the...|From the moment C...|    ['Tank', 'Mage']|      Mana|          3|           7|         7|              5|
                           |     0| 10.6.1|      Corki| 42|       Corki|the Daring Bombar...|The yordle pilot ...|        ['Marksman']|      Mana|          8|           3|         6|              6|
                           +------+-------+-----------+---+------------+--------------------+--------------------+--------------------+----------+-----------+---------                           ---+----------+---------------+

## Query the first item purchased by each champion with a win rate greater than 0.5:
                           +------------+--------------------+-------------+
                           |championName|          first_item|total_matches|
                           +------------+--------------------+-------------+
                           |      Aatrox|      Doran's Shield|           37|
                           |        Ahri|     Hextech GLP-800|           14|
                           |       Akali|    Hextech Gunblade|           35|
                           |     Alistar|Bulwark of the Mo...|           21|
                           |       Amumu|   Refillable Potion|            1|
                           |      Anivia|      Name Not Found|            1|
                           |       Annie|          Stormrazor|            1|
                           |    Aphelios|      Essence Reaver|           30|
                           |        Ashe|Blade of the Ruin...|           28|
                           |Aurelion Sol|     Hextech GLP-800|            2|
                           |        Azir|      Nashor's Tooth|           10|
                           |        Bard|          Redemption|           30|
                           |  Blitzcrank|Pauldrons of Whit...|           19|
                           |       Brand|        Luden's Echo|            4|
                           |       Braum|Pauldrons of Whit...|           11|
                           |     Caitlyn|       Infinity Edge|           18|
                           |     Camille|      Ravenous Hydra|           15|
                           |  Cassiopeia|    Seraph's Embrace|           15|
                           |    Cho'Gath|        Twin Shadows|            3|
                           |       Corki|       Trinity Force|            3|
                           +------------+--------------------+-------------+

#Practical 2: Using Pyspark to analyse data using Apache Spark

##Spark Initialization: Spark Context

SparkContext is the object that manages the connection to the clusters in Spark and coordinates running processes on the clusters themselves. SparkContext connects to cluster managers, which manage the actual executors that run the specific computations. Here’s a diagram from the Spark documentation to better visualize the architecture:

\ ![An empty blank notebook](fig/cluster-overview.png)

It may be automatically created (for instance if you call pyspark from the shells (the Spark context is then called sc).

But we haven’t set it up automatically in the Galaxy eduPortal, so you need to define it:

from pyspark import SparkContext
sc = SparkContext('local', 'pyspark tutorial') 
 

the driver (first argument) can be local[*], spark://”, **yarn, etc. What is available for you depends on how Spark has been deployed on the machine you use.
the second argument is the application name and is a human readable string you choose.
 

Because we do not specify any number of tasks for local, it means we will be using one only. To use a maximum of 2 tasks in parallel:

from pyspark import SparkContext
sc = SparkContext('local[2]', 'pyspark tutorial') 
If you wish to use all the available resource, you can simply use ‘*’ i.e.

from pyspark import SparkContext
sc = SparkContext('local[*]', 'pyspark tutorial') 
Please note that within one session, you cannot define several Spark context! So if you have tried the 3 previous SparkContext examples, don’t be surprised to get an error!

 

##Exercise 1: Map/Reduce

Let’s start with a map example where the goal is to convert temperature from Celcius to Kelvin.

Here it is how it translates in PySpark.

~~~
temp_c = [10, 3, -5, 25, 1, 9, 29, -10, 5]
rdd_temp_c = sc.parallelize(temp_c)
rdd_temp_K = rdd_temp_c.map(lambda x: x + 273.15).collect()
print(rdd_temp_K)   
~~~

You recognize the map function (please note it is not the pure python map function but PySpark map function). It acts here as the transformation function while collect is the action. It pulls all elements of the RDD to the driver.

####Remark:

It is often a very bad idea to pull all the elements of the RDD to the driver because we potentially handle very large amount of data. So instead we prefer to use take as you can specify how many elements you wish to pull from the RDD.

For instance to pull the first 3 elements only:

~~~
temp_c = [10, 3, -5, 25, 1, 9, 29, -10, 5]
rdd_temp_c = sc.parallelize(temp_c)
rdd_temp_K = rdd_temp_c.map(lambda x: x + 273.15).take(3)
print(rdd_temp_K)   
~~~

####Challenge:

~~~
def mod(x):
    import numpy as np
    return (x, np.mod(x, 2))

Nmax=1000
rdd = sc.parallelize(range(Nmax)).map(mod).take(5)
print(rdd)
~~~

Try the example above with different values for Nmax. Does it change the execution time if you take very large value for Nmax?

Why?

If you get the answer without looking for help, award yourself a pat on the back! If you need a pointer or two, have a look at the solution's section at the end of this document.

Now let’s take another example where we use map as the transformation and reduce for the action.
~~~
# we define a list of integers
numbers = [1, 4, 6, 2, 9, 10]

rdd_numbers=sc.parallelize(numbers)

# Use reduce to combine numbers
rdd_reduce = rdd_numbers.reduce(lambda x,y: "(" + str(x) + ", " + str(y) + ")")
print(rdd_reduce)
~~~

##Exercise 2: Create RDD from a file & explore the data 

In these exercises, we'll be using some example data from the **Five Thirty Eight** project. 538 publish articles based on the analysis of polling and other data and make many of the datasets they use available for further analysis.

Website: [http://fivethirtyeight.com/](http://fivethirtyeight.com/)

Data repository: [https://github.com/fivethirtyeight/data](https://github.com/fivethirtyeight/data)

In these exercises, we will be using the *Daily Show Guests* dataset, but you can do these exercises with any of the datasets you find useful or interesting.

First, download the `daily_show_guests.csv` file to your PC. Take a look at at and get a feel for the nature of the data it contains.

This should open up in your Spreadsheet application as it's a `csv` comma-separated variable file. This isn't a particularly large dataset, but it's large enough to demonstrate some important principles.

In your HortonWorks VM, launch **Apache Zeppelin**. This provides an online interactive Data Science notebook which uses the Python programming language in the background.

If the IP address of your instance is (for example: `10.123.231.19`, you'll find your notebook at web address:

`http://10.123.231.19:9995`

You should see something like this:

\ ![Zeppelin Notebook Splash Screen](fig/notebook_splash_screen.png)

From the Notebook drop-down select *Create new note+* and give the new notebook a suitable name:

![Naming the new notebook](fig/notebook_name.png)

You should get a new blank notebook:

\ ![An empty blank notebook](fig/empty_notebook.png)


##First steps with analysis

We need to get data using the 'Shell' interpreter. This will download the dataset into the Virtual Machine you created earlier:

Enter this code into the first blank section (we call these *paragraphs*) in the notebook.

~~~
%sh
wget https://raw.githubusercontent.com/fivethirtyeight/data/master/daily-show-guests/daily_show_guests.csv
~~~

Like this:

\ ![Notebook with first paragraph](fig/notebook_with_para.png)

Hit [SHIFT] and [ENTER] and you'll see:

~~~
--2016-11-13 20:01:09--  https://raw.githubusercontent.com/fivethirtyeight/data/master/daily-show-guests/daily_show_guests.csv
Resolving raw.githubusercontent.com... 151.101.60.133
Connecting to raw.githubusercontent.com|151.101.60.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 126723 (124K) [text/plain]
Saving to: “daily_show_guests.csv.1”
     0K .......... .......... .......... .......... .......... 40% 3.48M 0s
    50K .......... .......... .......... .......... .......... 80% 4.38M 0s
   100K .......... .......... ...                             100% 14.1M=0.03s
2016-11-13 20:01:10 (4.50 MB/s) - “daily_show_guests.csv.1” saved [126723/126723]
FINISHED   
~~~

Spark stores it's data in **Resilient Distributed datasets** (or RDDs for short), but first we need to get the data from the disk into HDFS, the **Hadoop Distributed File System** in our Virtual machine.
 
~~~
%sh
hadoop fs -put ~/daily_show_guests.csv /tmp
~~~

This will `put` the file we have just downloaded into the directory `tmp` within the HDFS.

If you make a mistake during this step or you put the wrong file in the HDFS, you can delete it by:

~~~
%sh
hdfs dfs -rm -r hdfs://sandbox.hortonworks.com/tmp/daily_show_guests.csv
~~~

 #####Next, to read a CSV file called "daily_show_guests.csv" into an RDD object called "my_rdd", we'll be using the *PySpark* Python interpreter, so we'll need to tell the Zeppelin notebook this by prefixing the command with `%pyspark`:

~~~
%pyspark
my_rdd = sc.textFile('hdfs://sandbox.hortonworks.com/tmp/daily_show_guests.csv')
~~~

We refer to the HDFS within this Sandbox as `hdfs://sandbox.hortonworks.com`. It's not a web address.

Let's make sure the data has been loaded into the `RDD` we called `my_rdd` by counting how many lines there are:

~~~
%pyspark
my_rdd = sc.textFile('hdfs://sandbox.hortonworks.com/tmp/daily_show_guests.csv')
my_rdd_filtered = my_rdd.filter( lambda x: len(x) > 0 )
counter = my_rdd_filtered.count()
print (counter)
~~~

It should return `2694` rows.

At this stage, these are just long rows of text and not split into the individual fields. If you recall, the fields are:

Header | Definition
-------|-----------
`YEAR` | The year the episode aired
`GoogleKnowlege_Occupation` | Their occupation or office, according to Google's Knowledge Graph or, 
if they're not in there, how Stewart introduced them on 
the program.
`Show` | Air date of episode. Not unique, as some shows had more than one guest
`Group` | A larger group designation for the occupation. For instance, us senators, us presidents, 
and former presidents are all under "politicians"
`Raw_Guest_List` | The person or list of people who appeared on the show, according to Wikipedia. 
The GoogleKnowlege_Occupation only refers to one of them in a given row. 

The RDD object my_rdd closely resembles a List of String objects, one object for each line in the dataset. We then use the take() method to print the first 5 elements of the RDD, so take a look at the first 5 rows of the RDD we just created:

~~~
%pyspark
print (my_rdd.take(5))
~~~

You'll note that the first row is a header row, but this is a bit of a mixed up format. It will make it much easier for us if we can turn each line into a LIST of other items. This will make it more straightforward to handle in Python.

To do this:

~~~
%pyspark
daily_show = my_rdd.map(lambda line: line.split(','))
~~~

and now:

~~~
%pyspark
print (daily_show.take(5))
~~~

will show:

~~~
[[u'YEAR', u'GoogleKnowlege_Occupation', u'Show', u'Group', u'Raw_Guest_List'], 
[u'1999', u'actor', u'1/11/99', u'Acting', u'Michael J. Fox'], 
[u'1999', u'Comedian', u'1/12/99', u'Comedy', u'Sandra Bernhard'], 
[u'1999', u'television actress', u'1/13/99', u'Acting', u'Tracey Ullman'], 
[u'1999', u'film actress', u'1/14/99', u'Acting', u'Gillian Anderson']]

~~~

Note the [SQUARE BRACKETS] around each line. This is a list. Each line is now a list of strings (Don't worry about the `u`- that just means they are stored in Unicode format).

To recap then, we:

1. Called the RDD function `map()` to specify we want the expression in the brackets to be applied to every line in our dataset 
2. Wrote a **lambda function**fig to split each line using the comma delimiter "," and assigned the resulting RDD to `daily_show`. In Python, a lambda function is just a small self-contained function or expression.
3. Called the RDD function `take()` on `daily_show` to display the first 5 rows of the resulting RDD 

Let's try something a bit more relevant. We need a tally (or simple histogram) of the number of guests each year in our dataset.

First, do this:

~~~
%pyspark
guest_tally = daily_show.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)
~~~

This uses both a `map()` and a `reduce()` function to create the tally.

The map part: `.map(lambda x: (x[0], 1))` goes down every row and creates a *tuple* (a key - value pair) consisting of the year and a value 1.

The reduce part: `reduceByKey(lambda x,y: x+y)` combines together all the tuples with the same key using the `x + y` function, ie. it adds them together.

Taking a look at the final results:

~~~
%pyspark
print (guest_tally.take(guest_tally.count()))
~~~

We see the resulting list of key-value pairs:

~~~
[(u'1999', 166), (u'2002', 159), (u'2000', 169), (u'2006', 161), (u'2004', 164), 
(u'2015', 100), (u'2008', 164), (u'2011', 163), (u'2013', 166), (u'2005', 162), 
(u'2003', 166), (u'2001', 157), (u'2007', 141), (u'YEAR', 1), (u'2014', 163), 
(u'2009', 163), (u'2010', 165), (u'2012', 164)]
FINISHED   
~~~

Unfortunately, Spark doesn't know how to handle the column headings so the element `(u'YEAR', 1)` needs to be removed.

Spark has a useful function called `filter` that allows us to create a new RDD from an existing one that contains only the elements we specify. We can create a function in Python that will skip over the line that contains `YEAR`. In the example below we refer to the first element of each line as `line[0]` because Python starts its element numbering at 0. The second element would be `line[1]` etc.

~~~
%pyspark
def filter_out_year(line):
    if line[0] == 'YEAR':
        return False
    else:
        return True
        
cleaned_daily_show = daily_show.filter (lambda line: filter_out_year(line))

print (cleaned_daily_show_tally.take(cleaned_daily_show_tally.count()))

~~~

Will now result in the complete tally of years and numbers of guests per year:

~~~
[(u'1999', 166), (u'2002', 159), (u'2000', 169), (u'2006', 161), (u'2004', 164), (u'2015', 100), (u'2008', 164), (u'2011', 163), (u'2013', 166), (u'2005', 162), (u'2003', 166), (u'2001', 157), (u'2007', 141), (u'2014', 163), (u'2009', 163), (u'2010', 165), (u'2012', 164)]
~~~

##Solution

###Solution to Challenge 1

Transformations are executed after actions and here we select 5 values only (take(5)) so whatever the number of Nmax, Spark executes exactly the same number of operations.

~~~
def mod(x):
  import numpy as np
  return (x, np.mod(x, 2))

Nmax= 10000000000
rdd = sc.parallelize(range(Nmax)).map(mod).take(5)
print(rdd)
~~~

##Further reading
















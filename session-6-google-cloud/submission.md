# 3. Create a new Dataflow project

## Step 2

- What directory has been created?

> Project created from Archetype in dir: /home/xividyzf/training-data-analyst/courses/data_analysis/lab2/newidea

- What package has been created inside the src directory?

> com.example.pipelinesrus.newidea

## Step 3

- What directory will get created?

> /home/xividyzf/training-data-analyst/courses/data_analysis/lab2/javahelp2

- What package will get created inside the src directory?

> com.google.cloud.training.dataanalyst.javahelp

# 4. Pipeline filtering

## Step 3

- What files are being read?

> `src/main/java/com/google/cloud/training/dataanalyst/javahelp/*.java`

- What is the search term? 

> "import"

- Where does the output go? 

> `/tmp/output.txt`

There are three apply statements in the pipeline:

- What does the first apply() do?

> Read lines from all `.java` files in the given path.

- What does the second apply() do? 

> It does a `grep`: output a line only if it contains the search term.

* Where does its input come from? 

> Each input is a line, which is an output of the first apply().

* What does it do with this input? 
> Search for the search term.

* What does it write to its output? 
> If the search term is found, output the input line; otherwise no output.

* Where does the output go to? 
> To the input of the next apply().

- What does the third apply() do? 

> Write its input to a text file `/tmp/output.txt` (without sharding).

# 5. Execute the pipeline locally

- Does the output seem logical?

Yes, the output is the same set of lines as what we get from unix `grep` command, but in different order.



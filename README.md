data-dec(k): your decorator platform for building python DAGs

# Goal

The goal was to create a CLI tool to mimic DBT functionality, but with python:
- Infer a DAG
- Define and write models automatically
- Assign tests
- run, test, or build the DAG from the cli
- Compile locally, execute remotely

# How it works

By calling the command `dec build`, the tool will compile all python code found in `<your_project>/models/**/*.py` and
`<your_project>/tests/**/*.py`. Decorators can be used to "register" a function as a models, a function as a test, 
apply a test to a function, or denote a dependendency on another model.

The code then gets compiled into a DAG that can be ran, tested, or visualized by the cli.

Anything defined in the spark api gets ran on databricks. Everything else is executed locally.

# Quick start

The first thing to do is to turn on our databricks cluster in dev. Find 
the `data_dec_cluster` cluster (id `0823-185034-5etcc6lh`) and turn it on. You'll need it soon.

## Project setup

data-dec uses a specific folder structure for a project to know where your data models and custom tests
live. So let's create a folder dedicated to this project somewhere on your device. Name it whatever you want and move into it.
The below example creates it in the Desktop folder of a mac.

```sh
cd ~/Desktop;
mkdir dec;
cd dec;
```

Now create a models/ dir with `mkdir models`. This will store all the python scripts that deal with data models.

Do the same but for tests. `mkdir tests`. Here you can add custom test functions.

Use the `touch` command to set up your `decor.yml` file (`touch decor.yml`). It will eventually store information about your "decorated"
project, but for now only defines a default profile to use. Open the file and paste the below info into it

```yml
version: 1

profile: dec
```

Great! Now we need to define the profile that we just referenced. The profile is used to configure 
what database and schema your models will be written to... for now. It'll eventually do more.

```yml
version: 1

dec:
  targets:
    dev:
      database: unity_dev_bi
      schema: dec_kyle
  default_target: dev
```

Feel free to use the database/schema shown above. If you change it that's fine, just make sure the 
database/schema exist. data-dec doesn't have the ability to create new ones yet.

That profile is for the data-dec project, but we also need to configure your databricks connect profile.
Open or create your `~/.databrickscfg` (`touch ~/.databricks.cfg`) file. Paste in the below info but replace
`<token>` with your personal databricks token. Leave the profile name "data-dec" as is.

```txt
[data-dec]
host = https://<your databricks host name>.cloud.databricks.com/
token = <token>
jobs-api-version = 2.0
cluster_id = 0823-185034-5etcc6lh
```

Now that the project is mostly set up we can install the data-dec package.

## Install

Start by creating a virtual env in the root of your project. Don't forget to activate it.

```sh
python3 -m venv venv;
source venv/bin/activate
```

The easiest way to install data-dec is to use pip's git api. If your git ssh token is
authorized you can install it like so 

```sh
pip install git+ssh://git@github.com/moranalytics/data-dec.git
```

If you don't have a ssh token authenticated you can install it with a git personal access token. 
Just follow [this guide](https://docs.readthedocs.io/en/stable/guides/private-python-packages.html).

You can now run `dec -h` and it should display the docs for the package entry point `dec`.

## create your first model

Models are functions that output a dataframe. They are decorated with another function that allows data-dec 
to compile them, eventually writing them to the database. The database and schema path are defined in your 
`profiles.yml` file and the table name is the name of the function.

Create a file `models/first_models.py` (`touch models/first_models.py`). Then define a function `model1` that pulls in the `dim_providers` 
table from dash 2.

```py
from data_dec.register import Register
from pyspark.sql import DataFrame

@Register.model()
def model1() -> DataFrame:
    df = spark.read.table('unity_dev_bi.dbt_kyle.dim_providers')
    return df.limit(5)
```

>[!Note]
> spark is created automatically. It's a remote connection to databricks. Anything executed
> using the spark api is ran on databricks. Everything else is local.

Now run `dec build`, the command to run then test all models. You should see an output like

```txt
Writing model 'model1' to table 'unity_dev_bi.dec_kyle.model1'
```

## Test your first model

There are two tests available by default in data-dec. `not_empty`, which checks if a table is empty, and
`not_null` which checks if a column is null. Lets add both of them to `model1`

```py
@Register.model()
@Register.model_test(test_name = 'not_empty')
@Register.model_test(test_name = 'not_null', column = 'npi')
def model1() -> DataFrame:
    df = spark.read.table('unity_dev_bi.dec_kyle.dim_providers')
    return df.limit(5)
```

You can see the test api is slightly different than registering models. It accepts the name of a test, 
which under the hood is a function name, and an optional set of arguments.

Run `dec build` or just `dec test` to execute them.

## Create and visualize your first dag

There's also a "reference" api in data-dec which defines a model that another model depends on. Currently
the reference is explicit, but at some point may become something we can infer.

Create `model2` which reads data from `model1`

```py
@Register.model()
@Register.reference('model1')
def model2() -> DataFrame:
    df = spark.read.table('unity_dev_bi.dec_kyle.model1')
    return df.limit(5)
```

Now `dec build` will run `model2` after `model1`. To visualize this you can actually draw the DAG. Run
`dec draw` and a graph will appear. When you're done looking at it make sure to close out of the window. 
Otherwise the process won't quit.

## Create your own test

Creating your own test is as simple as defining a function that accepts a `Model` class (required) and returns a boolean. You
can optionally define some arguments for it. Then you have to register it with the `test_function` decorator.

>[!Note]
> The `Model` class stores the model function as an object `fn`. To get the datafrmae from
> the model run `model.fn()`. This will most likely change in the future.

Create a file `tests/first_tests.py` (`touch tests/first_tests.py`) and paste in the below script.

```py
from data_dec.register import Register
from data_dec.entity import Model
from pyspark.sql import functions

@Register.test_function()
def npi_len_11(model: Model, column: str) -> bool:
    df = model.fn()
    df.filter(functions.length(df[column]) != 11)
    count = df.count()
    if count > 0:
        return False
    else:
        return True
```

The function checks to see if all npis are of length 11. If not the test fails.

Now you can decorate any model function to apply it. Just like you did with the `not_null` test for `model1`. Below is an example.

```py
@Register.model_test(test_name = 'npi_len_11', column = 'npi')
```

Try running `dec test` again and you should see that whatever model you apply it to should get tested.

### More efficient test strategy

The above method to get the dataframe of a model is a bit inefficient. Each test esssentially treats
the dataframe as a view and will rerun it everytime.

But the intent of data-dec's model tests is to test data that's already written. So you can access it
directly from databricks by querying it. Get the database path with the `model` attributes `database`, 
`schema`, and `name` then make a query with `spark.sql`.

```py
@Register.test_function()
def npi_len_11(model: Model, column: str) -> bool:
    path = '.'.join([model.database, model.schema, model.name])
    df = spark.sql(f"select * from {path}")

    # the rest of your test
```

# Caveates and pitfalls

## Local AND remote execution

Local execution can be seen as a feature or a flaw. I'm in the former camp, but I understand the consequences.
Notably, the EC2 instances that run this tool will bear the weight of processing files that cannot be
executed in the spark api. Like zip files. Everything else is executed remotely.

It also means using boto3 more often, and never using dbfs or Volumes directly. Only through s3. Personally,
the databricks file system can eat a shoe. 

## Decorators are multi-dimensional beings

I initially hoped decorators would give us a good way to register models, inject dependencies (think `ref` in dbt),
and dynamically test info. But they're a one way street to madness and should only be used to register metadata.

Essentially, decorators are functions that return functions. So `func2(func1)` is the same as

```py
@func2
def func1(): ...
```

When multiple decorators are stacked together their outpus begin to impact each other. There is no way to apply
multiple decorators and have them be ignorant of each other. The outcome of the below code is 
`func3(func2(func1))`, you cannot get something like `func3(func1)` and `func2(func1)` independently. So my "register" 
decorators don't modify the function in any way.

```py
@func2
@func3
def func1(): ...
```

Oh also order matters for decorators and the order is backwards. Silly.

## Compilation woes

Compiling code and allowing for developers to do whatever they want is difficult. Just ask DBT. 

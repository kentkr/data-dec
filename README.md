data-dec(k): your decorator platform for building python DAGs

# Goal

The goal was to create a CLI tool to mimic DBT functionality, but with python:
- Infer a DAG
- Define and write models automatically
- Assign tests
- run, test, or build the DAG from the cli
- Compile locally, execute remotely

# How it works

By calling the command `dec build`, the tool will compile all python code found in `<your_project>/models/**/*.py`. 
Decorators can be used to define those functions as models, apply tests to them, or denote a dependendency
on another model.

The code then gets compiled into a DAG that can be ran, tested, or visualized by the cli.

All "models" must return a pyspark dataframe to be written to databricks. Anything defined in the spark api 
gets ran on databricks. Everything else is executed locally.

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

Do the same but for tests. `mkdir tests`. Here you can add custom test functions..

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
host = https://forian-central-dev-engineering.cloud.databricks.com/
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

Creating your own test is as simple as defining a function that accepts a `Model` class (required), and some
other optional args. Then you have to assign it to a builtin data-dec class `TestFunctions` 

>[!Note]
> The `Model` class stores the model function as an object `fn`. To get the datafrmae from
> the model run `model.fn()`. This will most likely changei in the future.

Create a file `tests/first_tests.py` (`touch tests/first_tests.py`) and paste in the below script.

```py
from data_dec.entity import TestFunctions
from data_dec.entity import Model

def npi_len_11(model: Model, column: str) -> str:
    df = model.fn()
    rows = df.select(column).collect()
    for row in rows:
        length = len(row[column])
        if length != 11: 
            return f'Test fails: len {length} for {row.npi!r}'
    return 'Test passes'

TestFunctions.npi_len_11 = npi_len_11
```

The function checks to see if all npis are of length 11. You can pass in the `column` arg depending on
what the npi col is called.

Now you can decorate any model function to apply it. Just like you did with the `not_null` test for `model1`. Below is an example.

```py
@Register.model_test(test_name = 'npi_len_11', column = 'npi')
```

Try running `dec test` again and you should see that whatever model you apply it to should get tested.

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

data-dec(k): your decorator platform for building python DAGs

# Goal

The goal was to create a CLI tool to mimic DBT functionality, but with python:
- Infer a DAG
- Define and write models automatically
- Assign tests
- run, test, or build the DAG from the cli
- Compile locally, execute remotely

# How it works

By calling the command `dec build`, the tool will compile all python code found in `project/models/*.py`. There,
functions should be defined that return a spark DataFrame. Each of these functions should get a few decorators:
model, reference, or test.

Each decorator then assigns the function to an Entity class which stores the compiled code. From there, 
the cli can run each model, test them, or do both.

Anything defined in the spark api gets ran on databricks. Everything else is executed locally.

# Quick start

The smartest thing to start with is turn on that dang databricks cluster in dev. Find 
the dev full access cluster (id `1006-163626-a00gw6us`) and turn it on. You'll need it.

Since data-dec is a python module we'll need to install it locally. First clone the repository, set up
a local virtual environment, and install the requirements.

```sh
git clone ...;
cd data-dec;
python3 -m venv venv;
source venv/bin/activate;
pip3 install -r requirements.txt;
```

Now you should be able to call `dec`, the entry point for the tool, from the cli. Test it out
with the help flag. Run `dec -h`. You should see an output similar to this

```txt
usage: dec [-h] {build,run,test} ...

positional arguments:
  {build,run,test}
    build           Run and test models
    run             Run models
    test            Test models

options:
  -h, --help        show this help message and exit

```

Great! Now we need a way to connect to databricks to execute code remotely. We'll do that using the
`databricks-connect` module. It requires us to have a `~/.databrickscfg` file configured. You may already
have one set up but make sure to follow this step. It requires a few specific values. This is a POC after all.
What do you expect?

Create the file using
```sh
touch ~/.databrickscfg
```

Now copy and paste the below text into it to create a 'data-dec' databricks profile. 
Make sure to replace <token> with your token.

```
[data-dec]
host = https://forian-central-dev-engineering.cloud.databricks.com/
token = <token>
jobs-api-version = 2.0
cluster_id = 1006-163626-a00gw6us
```
> [!WARNING] 
> The cluster runtime must match `databricks-connect`. E.g. 13.3 runtime requires `databricks-connect==13.3.0`

It uses the dev full access cluster, so make sure it's on while using this tool.

Now create a project directory that will store all your data models, and a py file for
them.

```sh
mkdir project; mkdir project/models;
touch project/models/first_models.py;
```

Copy the below code into it.

```py
from data_dec.entity import Entity
from pyspark.sql import DataFrame

@Entity.register_model(path = 'unity_dev_bi.dbt_kyle.model1')
@Entity.register_test(test_name = 'not_empty')
@Entity.register_test(test_name = 'not_null')
def model1() -> DataFrame:
    df = spark.read.table('unity_prod_bi.bi_dashboard_blue.dim_providers')
    df = df.select(df['npi']).limit(10)
    return df
```

There are a few things happening here:
- We import our `Entity` class which'll keep metadata on all our entities
- We register the current function as a model and pass in it's database path (where it gets written to)
- We register the test 'not_empty' for this model
- We register the test 'not_null' for this model

All those registrations get compiled in the `Entity` class. Then the cli tool can infer how to run them. So
let's do that! Run `dec build`. You should see

```
Writing model 'model1' to table 'unity_dev_bi.dbt_kyle.model1'
Testing model model1
Testing: 'not_null'
Test passes
Testing: 'not_empty'
Test passes
```

As noted in the output, `model1` is now available at `unity_dev_bi.dbt_kyle.model1`.

Go forth and explore! Try creating your own model and specify the path you want it ot be written to.
Just know the `not_null` test only looks at the first column right now. Oh and you can't create
new tests yet...

Fin.

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

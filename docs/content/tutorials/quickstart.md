---
title: "QuickStart"
date: 2018-10-16T10:53:09+02:00
draft: true
---

Welcome to the QuickStart guide. In this guide we are going to cover how to use the tool to extract
a schema from a MongoDB database and review then apply the validation schema to MongoDB. We are going to
follow the following steps in the tutorial.

1. Preload some dummy data into a running MongoDB instance.
2. Run the tool against MongoDB to extract the Schema of the collections.
3. Apply the Schemas to the existing collections.

This QuickStart makes the assumption that you have a `MongoDB instance` running on `localhost` on port
`27017` that you can connect to with the `mongo` shell.

## Preloading Data

First connect to the MongoDB instance using the `mongo` shell from your commandline with the following command.

```shell
mongo
```

Next lets switch to the `quickstart` database that we will use in this example.

```shell
> use quickstart
```

Now lets insert a dummy document for each of the collections `sights` and `users`.

```bash
> db.users.insertOne({_id: 1, name: 'peter', address: { street: 'smiths road 16', city: 'london', country: 'uk' }})
```

Now lets insert a dummy document for each of the collections `sights` and `users`.

```bash
> db.sights.insertOne({_id: 1, user_id: 1, address: { street: 'smiths road 16', city: 'london', country: 'uk' }, name: "Peters house" })
```

We now have two simple collections primed with some sample data that we can use to generate the
MongoDB Json Schemas.

## Running the Schema Tool Extractor

In this step we are going to run the Schema Extractor Tool to generate the MongoDB Collection Schema
description.

This tool makes the assumption that you have a `Java 8` or higher installed to be able to run the
tool from the commandline. You need to be able to run the `java` command on the command line to
execute the tool.

Let's run the tool to extract the schemas

```bash
java -jar 
```
Build Containers:
```
sh build.sh
```

Run Containers:
```
sh run.sh
```

NiFi will be available at localhost:8080/nifi, and Zeppelin at localhost:8079.

NiFi Setup:

It'll take NiFi a minute or two to start. There's a race condition for creating tables before populating them, so ConvertJSONToSQL is disabled in the "Create & Populate Tables" processor group. Once NiFi has created the Phoenix tables, start the ConvertJSONToSQL processor and it will populate the CX_LOOKUP table.

Zeppelin Setup:

In Zeppelin, go to the "Setup" notebook, then open the Spark interpreter settings. Remove the Phoenix dependency, click save, then re-add it, save and restart the interpreter. Do the same for the JDBC interpreter.

Now return to "Setup" notebook and run the first two notes.

Demo Dashboard:

Go to the "Dashboard" notebook. Enter a Mosaic topology ID and hit enter.

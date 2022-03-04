## Running Metabase
In this project, we are using Metabase inside a Docker container. The [official documentation](https://www.metabase.com/docs/latest/operations-guide/running-metabase-on-docker.html) clearly mentioned a simple step to install Metabase in that way.

It is as simple as running:

```bash
docker run -d -p 3033:3000 --name metabase metabase/metabase
```

For the very first time of its execution, the above command downloads the latest Docker image available for Metabase before exposing the application on port `3033`.

On Metabase we can setup a connection to Redshift database as follow:

![Metabase -  Redshift connection](/images/redshift-metabase.png "Connecting Metabase to Redshift")

Note: replace the database credentials by the relevant values in your case. 


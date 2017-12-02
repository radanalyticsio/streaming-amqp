# Releasing

In order to create and publish the new version of Maven artifact you have to have an account on http://developer.jboss.org and have the permissions for releasing.

Next step is using the same credentials and saving them into your Maven settings. 

Here is the minimal example of the `~/.m2/settings.xml`:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                          https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>jboss-nexus-thirdparty</id>
      <username>your_username</username>
      <password>your_password</password>
    </server>
  </servers>
</settings>
```

If you don't want to save your credentials in plain-text, you may want to use the [password encryption](http://maven.apache.org/guides/mini/guide-encryption.html).

> Note that server id (`jboss-nexus-thirdparty`) has to match with the server id, defined in the [pom.xml](./pom.xml).

Next step is using the `maven-release-plugin` to do its job and push the artifact to the staging repository.

To do that, simply run:

```bash
mvn -Dresume=false release:prepare release:perform
```

It will ask about the version number you want to release, tag that will be created in the Git repository and the next snapshot version that will be put in the pom.xml.

The final step is doing the sanity checking in https://repository.jboss.org/nexus/index.html#stagingRepositories. Simply find your staging profile in the list (sorting by date helps), click on `close` and then on `release`.

That's it, you've just released into JBoss Nexus Maven repository. It syncs to Maven central, but it may take a day to get there.

You should also find your artifact in https://repository.jboss.org/nexus/service/local/repositories/releases/content/io/radanalytics/spark-streaming-amqp_2.11/

This is a custom NiFi processor to execute the Gdalwarp command. It is focused to
* Reproject input file
* Creates the internal raster tiling structure

This quick-and-dirty code has been developed starting form the standard NiFi ExecuteStreamCommand processor in order to evaluate the NiFi functionalities.

# Build

You need **java8** and **mvn 3.5**.

* Download this repo
* `cd` into this folder
* The run `mvn clean install` to produce the **.nar** file to be deployed inside a NiFi working installation
*  `mvn eclipse:clean eclipse:eclipse` to generate .project files for eclipse

The maven project was created following [this tutorial](https://community.hortonworks.com/articles/4318/build-custom-nifi-processor.html)

# Install

* the submodule nifi-gdalwarp-nar is used only to create the **.nar** file
* after having run a mvn clean install check the submodule nifi-gdalwarp-nar target folder
* Stop NiFi
* and copy the .nar file located in the targed dir in the lib folder of a NiFi working installation
* restart NiFi

At this point when adding a new processor you should find the custom processors listed in your NiFi instance.

The processor implementation entry points must be listed in the properties file `/src/main/resources/META-INF/services/org.apache.nifi.processor.Processor` of the submodule `nifi-gdalwarp-processors`

package com.creditkarma.blink.utils.gcs;

/**
 * Created by shengwei.wang on 12/16/16.
 */


import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.splunk.Command;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;

import com.splunk.*;

public class y {



    public static void main(String[] args) throws Exception {
        HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);

      /**
        Command command = Command.splunk("info").parse(args);

        try {
            Service.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
            Service serviceTLSv1_2 = Service.connect(command.opts);
            serviceTLSv1_2.login();
            System.out.println("\t Success!");
        } catch (RuntimeException e) {
            System.out.println("\t Failure! ");
        }
**/

        // Create a map of arguments and add login parameters
        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername("admin");
        loginArgs.setPassword("changemeyes");
        loginArgs.setHost("localhost");
        loginArgs.setPort(8089);

        // Create a Service instance and log in with the argument map
        Service service = Service.connect(loginArgs);


        // A second way to create a new Service object and log in
        // Service service = new Service("localhost", 8089);
        // service.login("admin", "changeme");

        // A third way to create a new Service object and log in
        // Service service = new Service(loginArgs);
        // service.login();

        // Print installed apps to the console to verify login
        for (Application app : service.getApplications().values()) {
            System.out.println(app.getName());
        }


        //Get the collection of indexes
        IndexCollection myIndexes = service.getIndexes();

        // Retrieve the index that was created earlier
        Index myIndex = service.getIndexes().get("test_index");
        //myIndex.clean(180);

// Retrieve properties
        System.out.println("Name:                " + myIndex.getName());
        System.out.println("Current DB size:     " + myIndex.getCurrentDBSizeMB() + "MB");
        System.out.println("Max hot buckets:     " + myIndex.getMaxHotBuckets());
        System.out.println("# of hot buckets:    " + myIndex.getNumHotBuckets());
        System.out.println("# of warm buckets:   " + myIndex.getNumWarmBuckets());
        System.out.println("Max data size:       " + myIndex.getMaxDataSize());
        System.out.println("Max total data size: " + myIndex.getMaxTotalDataSizeMB() + "MB");

// Modify a property and update the server
        myIndex.setMaxTotalDataSizeMB(myIndex.getMaxTotalDataSizeMB()-1);
        myIndex.update();
        System.out.println("Max total data size: " + myIndex.getMaxTotalDataSizeMB() + "MB");





        // Retrieve the collection of indexes, sorted by number of events
        IndexCollectionArgs indexcollArgs = new IndexCollectionArgs();
        indexcollArgs.setSortKey("totalEventCount");
        indexcollArgs.setSortDirection(IndexCollectionArgs.SortDirection.DESC);
        IndexCollection mIndexes = service.getIndexes(indexcollArgs);

// List the indexes and their event counts
        System.out.println("There are " + mIndexes.size() + " indexes:\n");
        for (Index entity: mIndexes.values()) {
            System.out.println("  " + entity.getName() + " (events: "
                    + entity.getTotalEventCount() + ")");
        }

        // Set up a timestamp
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        String date = sdf.format(new Date());

// Open a socket and stream
        Socket socket = myIndex.attach();
        try {
            OutputStream ostream = socket.getOutputStream();
            Writer out = new OutputStreamWriter(ostream, "UTF8");

            // Send events to the socket then close it
            out.write(date + "Event one!\r\n");
            out.write(date + "Event two!\r\n");
            out.flush();
        } finally {
            socket.close();
        }

// Create a simple search job
        String mySearch = "search * | head 5";
        Job job = service.getJobs().create(mySearch);

// Wait for the job to finish
        while (!job.isDone()) {
            Thread.sleep(500);
        }

// Display results
        InputStream results = job.getResults();
        //InputStream mResults = job.getEvents();
        String line = null;
        System.out.println("Results from the search job as XML:\n");
        BufferedReader br = new BufferedReader(new InputStreamReader(results, "UTF-8"));
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();



    }
}

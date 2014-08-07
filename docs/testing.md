# Compatibility testing for rmr 3.1.1
Please contribute with additional reports. To claim compatibility you need to run `R CMD  check path-to-rmr` successfully.
If you build your own Hadoop, see [Which Hadoop for rmr](https://github.com/RevolutionAnalytics/RHadoop/wiki/Which-Hadoop-for-rmr).
If you are interested in the compatibility chart for other releases, choose one from the drop down menu on the top left, under tags and find this document again (under docs). Not every release gets a complete round of testing, so typically a bug fix release (change in the third number only) is equally or more compatible than the previous release, even if we don't have the resources to test it directly. 

<table>
<thead>
<tr><th>Hadoop</th><th>R</th><th>OS</th><th>Notes</th><th>Reporter</th></tr>
</thead>
<tbody>
<tr><td>CDH4.6</td><td>R 3.0.2 (Revolution R 7.0)</td><td>CentOS 6.3</td><td>mr2</td><td><a href=mailto:rhadoop@revolutionanalytics.com>Revolution</a></td></tr>
</tbody>
</table>

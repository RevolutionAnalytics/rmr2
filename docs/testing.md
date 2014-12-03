# Testing for rmr2 3.3.0

In the table at the bottom we collect results concerning testing of rmr on a given combination of R/OS and Hadoop releases. We collect both positive and negative results if available. If a combination is not present in this table, it doesn't imply lack of compatibility. In case of negative results, they will be recorded but there is no guarantee that they will be fixed, albeit it's likely for current and common setups. In the early days `rmr` required a specific list of patches to be present in Hadoop to work. Currently, we expect it to work on any current or recent distibution by the Apache foundation, Hortonworks, Cloudera and MapR.

Testing is conducted by running `R CMD  check path-to-rmr` and requires an additional dependency, quickcheck, also downloadable from our wiki. Failures on producing documentation in legacy formats are not important and are ignored. Notes and warnings are not important in the sense that they do not determine success, but it may be helpful to report them in the issue tracker. Please contribute additional testing reports. 

If you are interested in the testing conducted on other releases, choose one from the drop down menu on the top left, under tags and find this document again (under docs). 



<table>

<thead>
<tr><th>Hadoop</th><th>R</th><th>OS</th><th>Notes</th><th>Reporter</th></tr>
</thead>

<tbody>
<tr>
  <td>Hadoop 2.4.0</td>
  <td>R 3.1.1 (Revolution R Open 8.0 beta)</td>
  <td>CentOS 6.4</td>
  <td></td>
  <td><a href=mailto:rhadoop@revolutionanalytics.com>Revolution</a></td>
</tr>
</tbody>

</table>

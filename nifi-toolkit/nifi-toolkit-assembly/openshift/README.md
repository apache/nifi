# Docker Image for OpenShift

This image can be used directly on OpenShift without needing any specific SCC or capabilities. File ownership conforms to OpenShift standard, allowing the image to be used in any namespace, whatever the Service Account uid.

The image is based on [ubi8/openjdk-11](https://catalog.redhat.com/software/containers/ubi8/openjdk-11/5dd6a4b45a13461646f677f4)

filegroup(
    name = "package-srcs",
    srcs = glob(["**"]),
    tags = ["automanaged"],
    visibility = ["//visibility:private"],
)

filegroup(
    name = "all-srcs",
    srcs = [
        ":package-srcs",
        "//k8s.io/noderesourcetopology-api/pkg/apis/topology:all-srcs",
        "//k8s.io/noderesourcetopology-api/pkg/generated/clientset/versioned:all-srcs",
        "//k8s.io/noderesourcetopology-api/pkg/generated/informers/externalversions:all-srcs",
        "//k8s.io/noderesourcetopology-api/pkg/generated/listers/topology/v1alpha1:all-srcs",
    ],
    tags = ["automanaged"],
    visibility = ["//visibility:public"],
)

# Terraform Provider Ceph RGW

This fork addresses few issues in the original provider:
* keys are not replaced on every user update
* user caps are applied correctly
  
It also supports `bucket_link`, `bucket_quota` and `quota` resources

## Requirements

- [Terraform](https://www.terraform.io/downloads.html) >= 1.0
- [Go](https://golang.org/doc/install) >= 1.18

## Building The Provider

1. Clone the repository
1. Enter the repository directory
1. Build the provider using the Go `install` command:

```shell
go install
```

## Using the provider locally

To distribute the provider locally, download the repository and run the following commands. Change `BINARY_ARCH` if you are not on apple silicon.
```
export PROVIDER_VERSION=1.0.0
export BINARY_ARCH=darwin_arm64
CGO_ENABLED=0 go build -o ~/.terraform.d/plugins/terraform.local/local/rgw/${PROVIDER_VERSION}/${BINARY_ARCH}/terraform-provider-rgw_v${PROVIDER_VERSION} -ldflags="-X 'main.Version=${PROVIDER_VERSION}'" main.go

```
Make sure to use the correct binary architecture

Add the following to your ~/.terraformrc
```
provider_installation {
  filesystem_mirror {
    path    = "/Users/%Me/.terraform.d/plugins"
  }
  direct {
    exclude = ["terraform.local/*/*"]
  }
}
```

## Developing the Provider

If you wish to work on the provider, you'll first need [Go](http://www.golang.org) installed on your machine (see [Requirements](#requirements) above).

To compile the provider, run `go install`. This will build the provider and put the provider binary in the `$GOPATH/bin` directory.

To generate or update documentation, run `go generate`.

In order to run the full suite of Acceptance tests, run `make testacc`.

*Note:* Acceptance tests create real resources, and often cost money to run.

```shell
make testacc
```

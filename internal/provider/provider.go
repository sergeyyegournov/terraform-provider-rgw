package provider

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ceph/go-ceph/rgw/admin"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure RgwProvider satisfies various provider interfaces.
var _ provider.Provider = &RgwProvider{}

// RgwProvider defines the provider implementation.
type RgwProvider struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

// RgwProviderModel describes the provider data model.
type RgwProviderModel struct {
	Endpoint  types.String `tfsdk:"endpoint"`
	AccessKey types.String `tfsdk:"access_key"`
	SecretKey types.String `tfsdk:"secret_key"`
}

type RgwClient struct {
	Admin *admin.API
	S3    *s3.Client
}

func (p *RgwProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "rgw"
	resp.Version = p.version
}

func (p *RgwProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"endpoint": schema.StringAttribute{
				MarkdownDescription: "RGW Endpoint URL. Can be set via env 'TF_PROVIDER_RGW_ENDPOINT'",
				Required:            true,
			},
			"access_key": schema.StringAttribute{
				MarkdownDescription: "RGW Access Key. Should be set via env 'TF_PROVIDER_RGW_ACCESS_KEY'",
				Optional:            true,
			},
			"secret_key": schema.StringAttribute{
				MarkdownDescription: "RGW Secret Key. Should be set via env 'TF_PROVIDER_RGW_SECRET_KEY'",
				Optional:            true,
				Sensitive:           true,
			},
		},
	}
}

func (p *RgwProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	// Retrieve provider data from configuration
	var data RgwProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if data.Endpoint.IsNull() {
		data.Endpoint = types.StringValue(os.Getenv("TF_PROVIDER_RGW_ENDPOINT"))
	}

	if data.AccessKey.IsNull() {
		data.AccessKey = types.StringValue(os.Getenv("TF_PROVIDER_RGW_ACCESS_KEY"))
	}

	if data.SecretKey.IsNull() {
		data.SecretKey = types.StringValue(os.Getenv("TF_PROVIDER_RGW_SECRET_KEY"))
	}

	// Create Ceph RGW Admin Client
	tflog.Debug(ctx, "Configuring Ceph RGW admin client")
	admin, err := admin.New(data.Endpoint.ValueString(), data.AccessKey.ValueString(), data.SecretKey.ValueString(), nil)
	if err != nil {
		resp.Diagnostics.AddError("could not create rgw admin client", err.Error())
		return
	}

	// Create s3 client
	tflog.Debug(ctx, "Configuring S3 client from AWS SDK")
	s3client := s3.New(s3.Options{
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     data.AccessKey.ValueString(),
				SecretAccessKey: data.SecretKey.ValueString(),
			}, nil
		}),
		EndpointResolver: s3.EndpointResolverFromURL(data.Endpoint.ValueString()),
		UsePathStyle:     true,
	})

	client := &RgwClient{
		Admin: admin,
		S3:    s3client,
	}

	resp.DataSourceData = client
	resp.ResourceData = client
}

func (p *RgwProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewBucketResource,
		NewUserResource,
		NewBucketPolicyResource,
		NewBucketLinkResource,
		NewUserQuotaResource
	}
}

func (p *RgwProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &RgwProvider{
			version: version,
		}
	}
}

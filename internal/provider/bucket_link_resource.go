package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/ceph/go-ceph/rgw/admin"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.ResourceWithConfigure = &BucketLinkResource{}

func NewBucketLinkResource() resource.Resource {
	return &BucketLinkResource{}
}

type BucketLinkResource struct {
	client *RgwClient
}

type BucketLinkResourceModel struct {
	UID         types.String `tfsdk:"uid"`
	Bucket      types.String `tfsdk:"bucket"`
	UnlinkToUID types.String `tfsdk:"unlink_to_uid"`
}

func (r *BucketLinkResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_bucket_link"
}

func (r *BucketLinkResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Ceph RGW Bucket Link",

		Attributes: map[string]schema.Attribute{
			"uid": schema.StringAttribute{
				MarkdownDescription: "The user ID to be linked with a bucket",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"bucket": schema.StringAttribute{
				MarkdownDescription: "The bucket name to link with a user",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"unlink_to_uid": schema.StringAttribute{
				MarkdownDescription: "The UID of a user to link bucket to when resource is destroyed",
				Optional:            true,
			},
		},
	}
}

func (r *BucketLinkResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*RgwClient)

	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *RgwClient, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)

		return
	}

	r.client = client
}

func (r *BucketLinkResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	// Read Terraform plan data into the model
	var data *BucketLinkResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Create API user object
	rgwBucketLink := admin.BucketLinkInput{
		Bucket: data.Bucket.ValueString(),
		UID:    data.UID.ValueString(),
	}

	// create bucket link
	err := r.client.Admin.LinkBucket(ctx, rgwBucketLink)
	if err != nil {
		resp.Diagnostics.AddError("could not create bucket link", err.Error())
		return
	}

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BucketLinkResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	// Read Terraform prior state data into the model
	var data *BucketLinkResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// get user's buckets
	buckets, err := r.client.Admin.ListUsersBuckets(ctx, data.UID.ValueString())
	if err != nil {
		if errors.Is(err, admin.ErrNoSuchUser) {
			// Remove bucket link from state
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("could not get user's buckets", err.Error())
		return
	}

	findString := func(slice []string, str string) bool {
		for _, item := range slice {
			if item == str {
				return true
			}
		}
		return false
	}

	if !findString(buckets, data.Bucket.ValueString()) {
		// Remove bucket link from state
		resp.State.RemoveResource(ctx)
		return
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BucketLinkResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Read Terraform plan data into the model
	var data *BucketLinkResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Currently there is nothing to update in place

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BucketLinkResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	// Read Terraform prior state data into the model
	var data *BucketLinkResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var err error
	if data.UnlinkToUID.IsNull() {
		// send delete request to api
		err = r.client.Admin.UnlinkBucket(ctx, admin.BucketLinkInput{
			Bucket: data.Bucket.ValueString(),
			UID:    data.UID.ValueString(),
		})
	} else {
		// send link request to api
		err = r.client.Admin.LinkBucket(ctx, admin.BucketLinkInput{
			Bucket: data.Bucket.ValueString(),
			UID:    data.UnlinkToUID.ValueString(),
		})
	}
	if err != nil && !errors.Is(err, admin.ErrNoSuchBucket) {
		resp.Diagnostics.AddError("could not delete bucket link", err.Error())
		return
	}
}

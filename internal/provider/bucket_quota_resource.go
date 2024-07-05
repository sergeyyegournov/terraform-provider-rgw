package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/ceph/go-ceph/rgw/admin"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.ResourceWithConfigure = &BucketQuotaResource{}

func NewBucketQuotaResource() resource.Resource {
	return &BucketQuotaResource{}
}

type BucketQuotaResource struct {
	client *RgwClient
}

type BucketQuotaResourceModel struct {
	Bucket     types.String `tfsdk:"bucket"`
	UID        types.String `tfsdk:"uid"`
	Enabled    types.Bool   `tfsdk:"enabled"`
	CheckOnRaw types.Bool   `tfsdk:"check_on_raw"`
	MaxSize    types.Int64  `tfsdk:"max_size"`
	MaxSizeKB  types.Int64  `tfsdk:"max_size_kb"`
	MaxObjects types.Int64  `tfsdk:"max_objects"`
}

func (r *BucketQuotaResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_bucket_quota"
}

func (r *BucketQuotaResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "This resource can be used to set individual quota for bucket. Refer to the Ceph RGW Admin Ops API documentation for values documentation. Upon deletion, quota is disabled.",

		Attributes: map[string]schema.Attribute{
			"bucket": schema.StringAttribute{
				MarkdownDescription: "The name of the bucket set the quota for.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"uid": schema.StringAttribute{
				MarkdownDescription: "The UID of the user to set the quota for.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"enabled": schema.BoolAttribute{
				MarkdownDescription: "Enable or disable the quota",
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(true),
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"check_on_raw": schema.BoolAttribute{
				MarkdownDescription: "???",
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(false),
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
			"max_size": schema.Int64Attribute{
				MarkdownDescription: "The maximum size of the quota",
				Computed:            true,
			},
			"max_size_kb": schema.Int64Attribute{
				MarkdownDescription: "The maximum size of the quota in kilobytes",
				Optional:            true,
				Computed:            true,
				Default:             int64default.StaticInt64(0),
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"max_objects": schema.Int64Attribute{
				MarkdownDescription: "The maximum number of objects in the quota",
				Optional:            true,
				Computed:            true,
				Default:             int64default.StaticInt64(-1),
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

func (r *BucketQuotaResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func rgwBucketQuotaFromSchemaQuota(data *BucketQuotaResourceModel) admin.QuotaSpec {
	enabled := data.Enabled.ValueBool()
	quota := admin.QuotaSpec{
		Bucket:     data.Bucket.ValueString(),
		UID:        data.UID.ValueString(),
		Enabled:    &enabled,
		CheckOnRaw: data.CheckOnRaw.ValueBool(),
	}

	// treat 0 as max_size quote disabled
	if !data.MaxSizeKB.IsNull() && data.MaxSizeKB.ValueInt64() != 0 {
		maxSizeKb := int(data.MaxSizeKB.ValueInt64())
		quota.MaxSizeKb = &maxSizeKb
	} else {
		maxSize := int64(-1)
		quota.MaxSize = &maxSize
	}

	if !data.MaxObjects.IsNull() {
		maxObjects := data.MaxObjects.ValueInt64()
		quota.MaxObjects = &maxObjects
	}

	return quota
}

func (r *BucketQuotaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	// Read Terraform plan data into the model
	var data *BucketQuotaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	quota := rgwBucketQuotaFromSchemaQuota(data)
	err := r.client.Admin.SetIndividualBucketQuota(ctx, quota)

	if err != nil {
		resp.Diagnostics.AddError("could not create bucket quota", err.Error())
		return
	}

	if data.MaxSizeKB.ValueInt64() != 0 {
		data.MaxSize = types.Int64Value(data.MaxSizeKB.ValueInt64() * 1024)
	} else {
		data.MaxSize = types.Int64Value(-1)
	}

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BucketQuotaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	// Read Terraform prior state data into the model
	var data *BucketQuotaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// prepare request attributes
	reqBucket := admin.Bucket{
		Bucket: data.Bucket.ValueString(),
	}

	// get bucket quota
	bucket, err := r.client.Admin.GetBucketInfo(ctx, reqBucket)

	if err != nil {
		if errors.Is(err, admin.ErrNoSuchBucket) {
			// Remove bucket quota from state
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("could not get bucket quota", err.Error())
		return
	}

	if bucket.BucketQuota.Enabled != nil {
		data.Enabled = types.BoolValue(*bucket.BucketQuota.Enabled)
	}
	data.CheckOnRaw = types.BoolValue(bucket.BucketQuota.CheckOnRaw)
	if bucket.BucketQuota.MaxSize != nil {
		data.MaxSize = types.Int64Value(*bucket.BucketQuota.MaxSize)
	}
	if bucket.BucketQuota.MaxSizeKb != nil {
		data.MaxSizeKB = types.Int64Value(int64(*bucket.BucketQuota.MaxSizeKb))
	}
	if bucket.BucketQuota.MaxObjects != nil {
		data.MaxObjects = types.Int64Value(*bucket.BucketQuota.MaxObjects)
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BucketQuotaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Read Terraform plan data into the model
	var data *BucketQuotaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	quota := rgwBucketQuotaFromSchemaQuota(data)
	err := r.client.Admin.SetIndividualBucketQuota(ctx, quota)

	if err != nil {
		resp.Diagnostics.AddError("could not modify bucket quota", err.Error())
		return
	}
	if data.MaxSizeKB.ValueInt64() != 0 {
		data.MaxSize = types.Int64Value(data.MaxSizeKB.ValueInt64() * 1024)
	} else {
		data.MaxSize = types.Int64Value(-1)
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BucketQuotaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	// Read Terraform prior state data into the model
	var data *BucketQuotaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	quota := rgwBucketQuotaFromSchemaQuota(data)
	f := false
	quota.Enabled = &f
	maxSize := int64(-1)
	quota.MaxSize = &maxSize
	quota.MaxSizeKb = nil
	maxObjects := int64(-1)
	quota.MaxObjects = &maxObjects

	err := r.client.Admin.SetIndividualBucketQuota(ctx, quota)
	if err != nil {
		resp.Diagnostics.AddError("could not modify bucket quota", err.Error())
		return
	}

	if err != nil && !errors.Is(err, admin.ErrNoSuchBucket) {
		resp.Diagnostics.AddError("could not delete bucket quota", err.Error())
		return
	}
}

package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/ceph/go-ceph/rgw/admin"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.ResourceWithConfigure = &UserQuotaResource{}

func NewUserQuotaResource() resource.Resource {
	return &UserQuotaResource{}
}

type UserQuotaResource struct {
	client *RgwClient
}

type UserQuotaResourceModel struct {
	UID                    types.String   `tfsdk:"uid"`
	Enabled                types.Bool     `tfsdk:"enabled"`
	CheckOnRaw             types.Bool     `tfsdk:"check_on_raw"`
	MaxSize                types.Int64    `tfsdk:"max_size"`
	MaxSizeKB              types.Int64    `tfsdk:"max_size_kb"`
	MaxObjects			   types.Int64    `tfsdk:"max_objects"`
}

func (r *UserQuotaResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_quota"
}

func (r *UserQuotaResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "This resource can be used to set the quota for a rgw user. Refer to the Ceph RGW Admin Ops API documentation for values documentation. Upon deletion, quota is disabled.",

		Attributes: map[string]schema.Attribute{
			"uid": schema.StringAttribute{
				MarkdownDescription: "The ID of the user to set the quota for.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"enabled": schema.BoolAttribute{
				MarkdownDescription: "Enable or disable the quota",
				Optional:            true,
				Default: booldefault.StaticBool(true),
			},
			"check_on_raw": schema.BoolAttribute{
				MarkdownDescription: "???",
				Optional:            true,
				Default: booldefault.StaticBool(false),
			},
			"max_size": schema.Int64Attribute{
				MarkdownDescription: "The maximum size of the quota",
				Optional:            true,
				Computed:            true,
			},
			"max_size_kb": schema.Int64Attribute{
				MarkdownDescription: "The maximum size of the quota in kilobytes",
				Optional:            true,
				Computed:            true,
			},
			"max_objects": schema.Int64Attribute{
				MarkdownDescription: "The maximum number of objects in the quota",
				Optional:            true,
				Computed:            true,
			},
		},
	}
}

func (r *UserQuotaResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func rgwQuotaFromSchemaQuota(data *UserQuotaResourceModel) admin.QuotaSpec {
	enabled := data.Enabled.ValueBool()
	quota := admin.QuotaSpec{
		UID:        data.UID.ValueString(),
		QuotaType:  "user",
		Enabled:    &enabled,
		CheckOnRaw: data.CheckOnRaw.ValueBool(),
	}

	if !data.MaxSize.IsNull() {
		maxSize := data.MaxSize.ValueInt64()
		quota.MaxSize = &maxSize
	}

	if !data.MaxSizeKB.IsNull() {
		maxSizeKb := int(data.MaxSizeKB.ValueInt64())
		quota.MaxSizeKb = &maxSizeKb
	}

	if !data.MaxObjects.IsNull() {
		maxObjects := data.MaxObjects.ValueInt64()
		quota.MaxObjects = &maxObjects
	}

	return quota
}

func (r *UserQuotaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	// Read Terraform plan data into the model
	var data *UserQuotaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	quota := rgwQuotaFromSchemaQuota(data)

	err := r.client.Admin.SetUserQuota(ctx, quota)
	if err != nil {
		resp.Diagnostics.AddError("could not create user quota", err.Error())
		return
	}

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *UserQuotaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	// Read Terraform prior state data into the model
	var data *UserQuotaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// prepare request attributes
	reqQuotaSpec := admin.QuotaSpec{
		UID: data.UID.ValueString(),
	}

	// get user quota
	quotaSpec, err := r.client.Admin.GetUserQuota(ctx, reqQuotaSpec)
	if err != nil {
		if errors.Is(err, admin.ErrNoSuchUser) {
			// Remove user from state
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("could not get user quota", err.Error())
		return
	}

	// check resource id
	if data.UID.ValueString() != quotaSpec.UID {
		resp.Diagnostics.AddError("api returned wrong user", fmt.Sprintf("expected user '%s', got '%s'", data.UID.ValueString(), quotaSpec.UID))
		return
	}

	if quotaSpec.Enabled != nil {
		data.Enabled = types.BoolValue(*quotaSpec.Enabled)
	}
	data.CheckOnRaw = types.BoolValue(quotaSpec.CheckOnRaw)
	if quotaSpec.MaxSize != nil {
		data.MaxSize = types.Int64Value(*quotaSpec.MaxSize)
	}
	if quotaSpec.MaxSizeKb != nil {
		data.MaxSizeKB = types.Int64Value(int64(*quotaSpec.MaxSizeKb))
	}
	if quotaSpec.MaxObjects != nil {
		data.MaxObjects = types.Int64Value(*quotaSpec.MaxObjects)
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *UserQuotaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Read Terraform plan data into the model
	var data *UserQuotaResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	quota := rgwQuotaFromSchemaQuota(data)

	err := r.client.Admin.SetUserQuota(ctx, quota)
	if err != nil {
		resp.Diagnostics.AddError("could not modify user quota", err.Error())
		return
	}

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *UserQuotaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	// Read Terraform prior state data into the model
	var data *UserQuotaResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	quota := rgwQuotaFromSchemaQuota(data)
	f := false
	quota.Enabled = &f

	err := r.client.Admin.SetUserQuota(ctx, quota)
	if err != nil {
		resp.Diagnostics.AddError("could not modify user quota", err.Error())
		return
	}

	if err != nil && !errors.Is(err, admin.ErrNoSuchUser) {
		resp.Diagnostics.AddError("could not delete user quota", err.Error())
		return
	}
}
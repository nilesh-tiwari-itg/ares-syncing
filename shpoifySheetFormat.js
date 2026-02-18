//chunk file read but not orphan in json creating then read-------------------------------------------------------------------------
import fs from "fs";
import path from "path";
import crypto from "crypto";
import readline from "readline";
import ExcelJS from "exceljs";
import { Readable } from "stream";

/** ---------- CONFIG ---------- **/
const OUTPUT_DIR = path.join(process.cwd(), "tmp", "shopify_exports");
const MF_NAMESPACE = "magento";

// ✅ FIX (1): base domain prefix for non-absolute image paths
// (You asked to add https://domain.com/ in Image Src + Variant Image)
const IMAGE_BASE_URL = "https://www.burkett.com/media/catalog/product";

// NOTE: We were NOT generating Variant Metafields earlier.
// Now we will generate ONLY ONE variant metafield: modal_number.
const MAX_PRODUCT_METAFIELDS = 180;
const MAX_VARIANT_METAFIELDS = 180;

/**
 * Shopify columns (unchanged)
 * We will append product metafield columns + (NOW) variant metafield columns.
 */
const SHOPIFY_COLUMNS = [
  "ID", "Handle", "Command", "Title", "Body HTML", "Vendor", "Type", "Tags", "Tags Command",
  "Created At", "Updated At", "Status", "Published", "Published At", "Published Scope",
  "Template Suffix", "Gift Card", "URL", "Total Inventory Qty", "Row #", "Top Row",
  "Category: ID", "Category: Name", "Category", "Custom Collections", "Smart Collections",
  "Image Type", "Image Src", "Image Command", "Image Position", "Image Width", "Image Height", "Image Alt Text",
  "Variant Inventory Item ID", "Variant ID", "Variant Command",
  "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value", "Option3 Name", "Option3 Value",
  "Variant Position", "Variant SKU", "Variant Barcode", "Variant Image", "Variant Weight", "Variant Weight Unit",
  "Variant Price", "Variant Compare At Price", "Variant Taxable", "Variant Tax Code",
  "Variant Inventory Tracker", "Variant Inventory Policy", "Variant Fulfillment Service",
  "Variant Requires Shipping", "Variant Shipping Profile", "Variant Inventory Qty", "Variant Inventory Adjust",
  "Variant Cost", "Variant HS Code", "Variant Country of Origin", "Variant Province of Origin",
  "Inventory Available: Shop location", "Inventory Available Adjust: Shop location",
  "Inventory On Hand: Shop location", "Inventory On Hand Adjust: Shop location",
  "Inventory Committed: Shop location", "Inventory Reserved: Shop location",
  "Inventory Damaged: Shop location", "Inventory Damaged Adjust: Shop location",
  "Inventory Safety Stock: Shop location", "Inventory Safety Stock Adjust: Shop location",
  "Inventory Quality Control: Shop location", "Inventory Quality Control Adjust: Shop location",
  "Inventory Incoming: Shop location",
  "Included / test cat", "Price / test cat", "Compare At Price / test cat",
  "Metafield: title_tag [string]", "Metafield: description_tag [string]",
];

/**
 * CLIENT REQUIRED METAFIELDS (product only)
 */
const PRODUCT_METAFIELD_SPECS = [
  { key: "price", type: "single_line_text_field", source: "price" },
  { key: "price_change_date", type: "date", source: "price_change_date" },
  { key: "price_change_amt", type: "single_line_text_field", source: "price_change_amt" },
  { key: "made_in_usa", type: "boolean", source: "made_in_usa" },
  { key: "energy_star", type: "boolean", source: "energy_star" },
  { key: "product_certs", type: "list.single_line_text_field", source: "product_certs" },

  { key: "shipping_speed", type: "single_line_text_field", source: "shipping_speed" },
  { key: "condtition", type: "single_line_text_field", source: "condtition" }, // client typo kept
  { key: "condition", type: "single_line_text_field", source: "condition" },   // sheet column exists
  { key: "uom", type: "single_line_text_field", source: "uom" },
  { key: "freight_class", type: "single_line_text_field", source: "freight_class" },
  { key: "must_ship_freight", type: "single_line_text_field", source: "must_ship_freight" },
  { key: "discon_replacement", type: "single_line_text_field", source: "discon_replacement" },
  { key: "image_note", type: "single_line_text_field", source: "image_note" },

  { key: "product_cert", type: "single_line_text_field", source: "product_cert" }, // if exists
  { key: "product_certs_text", type: "single_line_text_field", source: "product_certs" }, // optional extra
  { key: "filter_exterior_finish", type: "single_line_text_field", source: "filter_exterior_finish" },
  { key: "filter_interior_finish", type: "single_line_text_field", source: "filter_interior_finish" },
  { key: "filter_width_side_side", type: "single_line_text_field", source: "filter_width_side_side" },
  { key: "filter_doortype", type: "single_line_text_field", source: "filter_doortype" },
  { key: "filter_doorqty", type: "single_line_text_field", source: "filter_doorqty" },
  { key: "filter_depth_front_back", type: "single_line_text_field", source: "filter_depth_front_back" },
  { key: "filter_door_swing", type: "single_line_text_field", source: "filter_door_swing" },
  { key: "filter_refrigeration_location", type: "single_line_text_field", source: "filter_refrigeration_location" },
  { key: "filter_top_finish", type: "single_line_text_field", source: "filter_top_finish" },
  { key: "filter_dispenser_type", type: "single_line_text_field", source: "filter_dispenser_type" },
  { key: "filter_storage_capacity", type: "single_line_text_field", source: "filter_storage_capacity" },
  { key: "filter_daily_production", type: "single_line_text_field", source: "filter_daily_production" },
  { key: "filter_cooling", type: "single_line_text_field", source: "filter_cooling" },
  { key: "filter_compressor_horsepower", type: "single_line_text_field", source: "filter_compressor_horsepower" },
  { key: "filter_drawer_quantity", type: "single_line_text_field", source: "filter_drawer_quantity" },
  { key: "filter_height_top_bottom", type: "single_line_text_field", source: "filter_height_top_bottom" },
  { key: "filter_shelf_quantity", type: "single_line_text_field", source: "filter_shelf_quantity" },
  { key: "filter_service_type", type: "single_line_text_field", source: "filter_service_type" },
  { key: "filter_display_front_style", type: "single_line_text_field", source: "filter_display_front_style" },
  { key: "filter_installation_type", type: "single_line_text_field", source: "filter_installation_type" },
  { key: "filter_working_tub_capacity", type: "single_line_text_field", source: "filter_working_tub_capacity" },
  { key: "filter_keg_colpertower", type: "single_line_text_field", source: "filter_keg_colpertower" },
  { key: "filter_keg_barrel_style", type: "single_line_text_field", source: "filter_keg_barrel_style" },
  { key: "filter_keg_faucets", type: "single_line_text_field", source: "filter_keg_faucets" },
  { key: "filter_access_type", type: "single_line_text_field", source: "filter_access_type" },
  { key: "filter_sections", type: "single_line_text_field", source: "filter_sections" },
  { key: "filter_work_surface", type: "single_line_text_field", source: "filter_work_surface" },
  { key: "filter_nom_long_side", type: "single_line_text_field", source: "filter_nom_long_side" },
  { key: "filter_nom_height", type: "single_line_text_field", source: "filter_nom_height" },
  { key: "filter_nom_short_side", type: "single_line_text_field", source: "filter_nom_short_side" },
  { key: "filter_floor", type: "single_line_text_field", source: "filter_floor" },
  { key: "filter_heat_source", type: "single_line_text_field", source: "filter_heat_source" },
  { key: "filter_ignition_type", type: "single_line_text_field", source: "filter_ignition_type" },
  { key: "filter_rack_positions", type: "single_line_text_field", source: "filter_rack_positions" },
  { key: "filter_controls", type: "single_line_text_field", source: "filter_controls" },
  { key: "filter_fs_hotelpan_cap", type: "single_line_text_field", source: "filter_fs_hotelpan_cap" },
  { key: "filter_fs_sheetpan_cap", type: "single_line_text_field", source: "filter_fs_sheetpan_cap" },
  { key: "filter_steam_type", type: "single_line_text_field", source: "filter_steam_type" },
  { key: "filter_griddle_area", type: "single_line_text_field", source: "filter_griddle_area" },
  { key: "filter_griddle_plate_thickness", type: "single_line_text_field", source: "filter_griddle_plate_thickness" },
  { key: "filter_burner_quantity", type: "single_line_text_field", source: "filter_burner_quantity" },
  { key: "filter_oven_capacity", type: "single_line_text_field", source: "filter_oven_capacity" },
  { key: "filter_proofer_capacity", type: "single_line_text_field", source: "filter_proofer_capacity" },
  { key: "filter_oven_size", type: "single_line_text_field", source: "filter_oven_size" },
  { key: "filter_production_per_hour", type: "single_line_text_field", source: "filter_production_per_hour" },
  { key: "filter_bread_or_bagel", type: "single_line_text_field", source: "filter_bread_or_bagel" },
  { key: "filter_toaster_opening", type: "single_line_text_field", source: "filter_toaster_opening" },
  { key: "filter_fat_capacity", type: "single_line_text_field", source: "filter_fat_capacity" },
  { key: "filter_oil_filter", type: "single_line_text_field", source: "filter_oil_filter" },
  { key: "filter_hotdog_capacity", type: "single_line_text_field", source: "filter_hotdog_capacity" },
  { key: "filter_burner_type", type: "single_line_text_field", source: "filter_burner_type" },
  { key: "filter_watts", type: "single_line_text_field", source: "filter_watts" },
  { key: "filter_usage_level", type: "single_line_text_field", source: "filter_usage_level" },
  { key: "filter_tank_capacity", type: "single_line_text_field", source: "filter_tank_capacity" },
  { key: "filter_tank_quantity", type: "single_line_text_field", source: "filter_tank_quantity" },
  { key: "filter_water_fill", type: "single_line_text_field", source: "filter_water_fill" },
  { key: "filter_pop_kettle_size", type: "single_line_text_field", source: "filter_pop_kettle_size" },
  { key: "filter_range_surface", type: "single_line_text_field", source: "filter_range_surface" },
  { key: "filter_range_back", type: "single_line_text_field", source: "filter_range_back" },
  { key: "filter_cooked_rice_capacity", type: "single_line_text_field", source: "filter_cooked_rice_capacity" },
  { key: "filter_uncooked_rice_capacity", type: "single_line_text_field", source: "filter_uncooked_rice_capacity" },
  { key: "filter_plate_quantity", type: "single_line_text_field", source: "filter_plate_quantity" },
  { key: "filter_pan_capacity", type: "single_line_text_field", source: "filter_pan_capacity" },
  { key: "filter_drain_connection", type: "single_line_text_field", source: "filter_drain_connection" },
  { key: "filter_internal_capacity", type: "single_line_text_field", source: "filter_internal_capacity" },
  { key: "filter_kettle_jacket", type: "single_line_text_field", source: "filter_kettle_jacket" },
  { key: "filter_burner_ring_quantity", type: "single_line_text_field", source: "filter_burner_ring_quantity" },
  { key: "filter_base_type", type: "single_line_text_field", source: "filter_base_type" },
  { key: "filter_tilt_mechanism", type: "single_line_text_field", source: "filter_tilt_mechanism" },
  { key: "filter_pan_shape", type: "single_line_text_field", source: "filter_pan_shape" },
  { key: "filter_fryer_quantity", type: "single_line_text_field", source: "filter_fryer_quantity" },
  { key: "filter_waffle_shape", type: "single_line_text_field", source: "filter_waffle_shape" },
  { key: "filter_size", type: "single_line_text_field", source: "filter_size" },
  { key: "filter_working_height", type: "single_line_text_field", source: "filter_working_height" },
  { key: "filter_arm_style", type: "single_line_text_field", source: "filter_arm_style" },
  { key: "filter_lamp_finish", type: "single_line_text_field", source: "filter_lamp_finish" },
  { key: "filter_bulb_quantity", type: "single_line_text_field", source: "filter_bulb_quantity" },
  { key: "filter_support_location", type: "single_line_text_field", source: "filter_support_location" },
  { key: "filter_slide_capacity", type: "single_line_text_field", source: "filter_slide_capacity" },
  { key: "filter_insulation", type: "single_line_text_field", source: "filter_insulation" },
  { key: "filter_centers_size", type: "single_line_text_field", source: "filter_centers_size" },
  { key: "filter_color", type: "single_line_text_field", source: "filter_color" },
  { key: "filter_operation", type: "single_line_text_field", source: "filter_operation" },
  { key: "filter_hot_or_cold", type: "single_line_text_field", source: "filter_hot_or_cold" },
  { key: "filter_thermometer_type", type: "single_line_text_field", source: "filter_thermometer_type" },
  { key: "filter_internal_configuration", type: "single_line_text_field", source: "filter_internal_configuration" },
  { key: "filter_shelf_type", type: "single_line_text_field", source: "filter_shelf_type" },
  { key: "filter_heated_unheated", type: "single_line_text_field", source: "filter_heated_unheated" },
  { key: "filter_plate_diameter", type: "single_line_text_field", source: "filter_plate_diameter" },
  { key: "filter_hs_sheetpan_capacity", type: "single_line_text_field", source: "filter_hs_sheetpan_capacity" },
  { key: "filter_pump_style", type: "single_line_text_field", source: "filter_pump_style" },
  { key: "filter_dispenser_quantity", type: "single_line_text_field", source: "filter_dispenser_quantity" },
  { key: "filter_warmer_type", type: "single_line_text_field", source: "filter_warmer_type" },
  { key: "filter_cold_pan_capacity", type: "single_line_text_field", source: "filter_cold_pan_capacity" },
  { key: "filter_hot_pan_capacity", type: "single_line_text_field", source: "filter_hot_pan_capacity" },
  { key: "filter_well_type", type: "single_line_text_field", source: "filter_well_type" },
  { key: "filter_drainboard", type: "single_line_text_field", source: "filter_drainboard" },
  { key: "filter_splash_height", type: "single_line_text_field", source: "filter_splash_height" },
  { key: "filter_sink_compartment_qty", type: "single_line_text_field", source: "filter_sink_compartment_qty" },
  { key: "filter_dishmachine_temp", type: "single_line_text_field", source: "filter_dishmachine_temp" },
  { key: "filter_dish_rack_capacity", type: "single_line_text_field", source: "filter_dish_rack_capacity" },
  { key: "filter_dishmachine_arm_type", type: "single_line_text_field", source: "filter_dishmachine_arm_type" },
  { key: "filter_dishrack_compartments", type: "single_line_text_field", source: "filter_dishrack_compartments" },
  { key: "filter_inside_rack_height", type: "single_line_text_field", source: "filter_inside_rack_height" },
  { key: "filter_dish_machine_location", type: "single_line_text_field", source: "filter_dish_machine_location" },
  { key: "filter_operation_direction", type: "single_line_text_field", source: "filter_operation_direction" },
  { key: "filter_design", type: "single_line_text_field", source: "filter_design" },
  { key: "filter_ice_type", type: "single_line_text_field", source: "filter_ice_type" },
  { key: "filter_lead_compliance", type: "single_line_text_field", source: "filter_lead_compliance" },
  { key: "filter_faucet_centers", type: "single_line_text_field", source: "filter_faucet_centers" },
  { key: "filter_faucet_spout_length", type: "single_line_text_field", source: "filter_faucet_spout_length" },
  { key: "filter_garb_disposal_fit", type: "single_line_text_field", source: "filter_garb_disposal_fit" },
  { key: "filter_grease_flow_rate", type: "single_line_text_field", source: "filter_grease_flow_rate" },
  { key: "filter_motor_horsepower", type: "single_line_text_field", source: "filter_motor_horsepower" },
  { key: "filter_max_opening_height", type: "single_line_text_field", source: "filter_max_opening_height" },
  { key: "filter_number_of_motors", type: "single_line_text_field", source: "filter_number_of_motors" },
  { key: "filter_container_finish", type: "single_line_text_field", source: "filter_container_finish" },
  { key: "filter_motor_type", type: "single_line_text_field", source: "filter_motor_type" },
  { key: "filter_beverage_type", type: "single_line_text_field", source: "filter_beverage_type" },
  { key: "filter_glass_height", type: "single_line_text_field", source: "filter_glass_height" },
  { key: "filter_glass_diameter", type: "single_line_text_field", source: "filter_glass_diameter" },
  { key: "filter_number_of_hoppers", type: "single_line_text_field", source: "filter_number_of_hoppers" },
  { key: "filter_grinder_type", type: "single_line_text_field", source: "filter_grinder_type" },
  { key: "filter_sink_dimensions", type: "single_line_text_field", source: "filter_sink_dimensions" },
  { key: "filter_fill_type", type: "single_line_text_field", source: "filter_fill_type" },
  { key: "filter_spindle_qty", type: "single_line_text_field", source: "filter_spindle_qty" },
  { key: "filter_roller", type: "single_line_text_field", source: "filter_roller" },
  { key: "filter_blending_capacity", type: "single_line_text_field", source: "filter_blending_capacity" },
  { key: "filter_shaft_size", type: "single_line_text_field", source: "filter_shaft_size" },
  { key: "filter_attachments", type: "single_line_text_field", source: "filter_attachments" },
  { key: "filter_surface_size", type: "single_line_text_field", source: "filter_surface_size" },
  { key: "filter_legal_for_trade", type: "single_line_text_field", source: "filter_legal_for_trade" },
  { key: "filter_weight_capacity", type: "single_line_text_field", source: "filter_weight_capacity" },
  { key: "filter_faucet_spout_type", type: "single_line_text_field", source: "filter_faucet_spout_type" },
  { key: "filter_container_capacity", type: "single_line_text_field", source: "filter_container_capacity" },
  { key: "filter_warmer_quantity", type: "single_line_text_field", source: "filter_warmer_quantity" },
  { key: "filter_btu", type: "single_line_text_field", source: "filter_btu" },
  { key: "filter_output", type: "single_line_text_field", source: "filter_output" },
  { key: "filter_drive_type", type: "single_line_text_field", source: "filter_drive_type" },
  { key: "filter_knife_size", type: "single_line_text_field", source: "filter_knife_size" },
  { key: "filter_feed_type", type: "single_line_text_field", source: "filter_feed_type" },
  { key: "filter_cutter_type", type: "single_line_text_field", source: "filter_cutter_type" },
  { key: "filter_seat_style", type: "single_line_text_field", source: "filter_seat_style" },
  { key: "filter_back_style", type: "single_line_text_field", source: "filter_back_style" },
  { key: "filter_glass_type", type: "single_line_text_field", source: "filter_glass_type" },
  { key: "filter_ship_assembly", type: "single_line_text_field", source: "filter_ship_assembly" },
  { key: "filter_length", type: "single_line_text_field", source: "filter_length" },
  { key: "filter_shape", type: "single_line_text_field", source: "filter_shape" },
  { key: "filter_dough_capacity", type: "single_line_text_field", source: "filter_dough_capacity" },
  { key: "filter_bowl_capacity", type: "single_line_text_field", source: "filter_bowl_capacity" },
  { key: "filter_knife_type", type: "single_line_text_field", source: "filter_knife_type" },
  { key: "filter_handle_finish", type: "single_line_text_field", source: "filter_handle_finish" },
  { key: "filter_blade_length", type: "single_line_text_field", source: "filter_blade_length" },
  { key: "filter_heater_location", type: "single_line_text_field", source: "filter_heater_location" },
  { key: "filter_weight_display", type: "single_line_text_field", source: "filter_weight_display" },
  { key: "filter_urn_qty", type: "single_line_text_field", source: "filter_urn_qty" },
  { key: "filter_brewer_qty", type: "single_line_text_field", source: "filter_brewer_qty" },
  { key: "filter_brewing_capacity", type: "single_line_text_field", source: "filter_brewing_capacity" },
  { key: "filter_bowl_qty", type: "single_line_text_field", source: "filter_bowl_qty" },
  { key: "filter_heater_bar_qty", type: "single_line_text_field", source: "filter_heater_bar_qty" },
  { key: "filter_base_finish", type: "single_line_text_field", source: "filter_base_finish" },
  { key: "filter_broiler_area", type: "single_line_text_field", source: "filter_broiler_area" },
  { key: "filter_power_level", type: "single_line_text_field", source: "filter_power_level" },
  { key: "filter_deck_qty", type: "single_line_text_field", source: "filter_deck_qty" },
  { key: "filter_utensil_capacity", type: "single_line_text_field", source: "filter_utensil_capacity" },
  { key: "filter_stackable", type: "single_line_text_field", source: "filter_stackable" },
  { key: "filter_material", type: "single_line_text_field", source: "filter_material" },
  { key: "filter_edge", type: "single_line_text_field", source: "filter_edge" },
  { key: "filter_collection", type: "single_line_text_field", source: "filter_collection" },
  { key: "filter_capacity", type: "single_line_text_field", source: "filter_capacity" },
  { key: "filter_style", type: "single_line_text_field", source: "filter_style" },
  { key: "filter_gauge", type: "single_line_text_field", source: "filter_gauge" },
  { key: "filter_features", type: "single_line_text_field", source: "filter_features" },
  { key: "filter_power_type", type: "single_line_text_field", source: "filter_power_type" },
  { key: "filter_warmer_setup", type: "single_line_text_field", source: "filter_warmer_setup" },
  { key: "filter_compartment_quantity", type: "single_line_text_field", source: "filter_compartment_quantity" },
  { key: "filter_bun_capacity", type: "single_line_text_field", source: "filter_bun_capacity" },
  { key: "filter_pan_size", type: "single_line_text_field", source: "filter_pan_size" },
  { key: "filter_wet_dry_operation", type: "single_line_text_field", source: "filter_wet_dry_operation" },
  { key: "filter_roller_size", type: "single_line_text_field", source: "filter_roller_size" },
  { key: "filter_keg_capacity", type: "single_line_text_field", source: "filter_keg_capacity" },
  { key: "filter_sink_length", type: "single_line_text_field", source: "filter_sink_length" },
  { key: "filter_sink_depth", type: "single_line_text_field", source: "filter_sink_depth" },
  { key: "filter_sink_width", type: "single_line_text_field", source: "filter_sink_width" },
  { key: "filter_booster_included", type: "single_line_text_field", source: "filter_booster_included" },
  { key: "filter_sink_included", type: "single_line_text_field", source: "filter_sink_included" },
  { key: "filter_faucet_included", type: "single_line_text_field", source: "filter_faucet_included" },
  { key: "filter_with_lights", type: "single_line_text_field", source: "filter_with_lights" },
  { key: "filter_pan_type", type: "single_line_text_field", source: "filter_pan_type" },
  { key: "filter_pan_rack_capacity", type: "single_line_text_field", source: "filter_pan_rack_capacity" },
  { key: "directional_operation", type: "single_line_text_field", source: "directional_operation" },

  // ✅ (1) NEW product metafield: short_description (sheet column: short_description)
  { key: "short_description", type: "single_line_text_field", source: "short_description" },

  // ✅ (2) NEW product metafield: warranty_product (sheet column: warranty_product)
  { key: "warranty_product", type: "single_line_text_field", source: "warranty_product" },
  { key: "shipperhq_shipping_group", type: "single_line_text_field", source: "shipperhq_shipping_group" },

  // ✅ NEW (your request): store parent configurable SKU on the product (not variants)
  // Only populate for configurable parents.
  { key: "main_product_sku", type: "single_line_text_field", source: "sku", onlyForConfigurable: true },
];

/**
 * ✅ (3) Variant metafield specs
 */
// ✅ Variant metafields = (your existing variant fields) + (all product metafields copied onto variants)
// Rule: values come from VARIANT row (child row). Skip "onlyForConfigurable" specs (product-only).
const VARIANT_METAFIELD_SPECS = (() => {
  const base = [
    { key: "modal_number", type: "single_line_text_field", source: "modal_number", fallbackSource: "model_number" },
    { key: "price", type: "single_line_text_field", source: "price" },
  ];

  // Copy ALL product metafields to variants as well (so variant rows can carry filter_material, etc.)
  for (const pm of PRODUCT_METAFIELD_SPECS) {
    if (pm?.onlyForConfigurable) continue; // keep product-only metafields product-only
    base.push({
      key: pm.key,
      type: pm.type,
      source: pm.source,
    });
  }

  // De-dupe (same key+type repeated)
  const seen = new Set();
  return base.filter((s) => {
    const k = `${s.key}__${s.type}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
})();
/** ---------- HELPERS ---------- **/
function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function isEmpty(v) {
  return v === null || v === undefined || String(v).trim() === "";
}

function toStr(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function safeNumber(v) {
  const n = parseFloat(String(v));
  return Number.isFinite(n) ? n : 0;
}

function slugify(text) {
  return String(text || "")
    .toLowerCase()
    .trim()
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function isAbsoluteUrl(u) {
  return /^https?:\/\//i.test(String(u || ""));
}

// ✅ FIX (1): if image path is relative, prefix IMAGE_BASE_URL
function normalizeImageUrl(src) {
  const s = toStr(src).trim();
  if (!s) return "";
  if (isAbsoluteUrl(s)) return s;

  const base = String(IMAGE_BASE_URL || "").trim();
  if (!base) return s;

  const baseFixed = base.endsWith("/") ? base : base + "/";
  const pathFixed = s.startsWith("/") ? s.slice(1) : s;
  return baseFixed + pathFixed;
}

// ✅ FIX (your issue #1): parent_sku sometimes contains "OLD_PARENT,NEW_PARENT"
// Rule requested: take the second/new one (safe: take the last non-empty).
function normalizeParentSku(parentSkuRaw) {
  const raw = toStr(parentSkuRaw).trim();
  if (!raw) return "";
  if (!raw.includes(",")) return raw;
  const parts = raw.split(",").map((x) => x.trim()).filter(Boolean);
  if (!parts.length) return "";
  return parts[parts.length - 1];
}

function toMetafieldKey(colName) {
  const k = String(colName || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return k.length > 30 ? k.slice(0, 30) : k || "field";
}

function asTags(categories, categoriesStoreName) {
  const parts = [];
  if (!isEmpty(categories)) parts.push(...String(categories).split(","));
  if (!isEmpty(categoriesStoreName)) parts.push(...String(categoriesStoreName).split(","));
  const tags = parts.map((t) => t.trim()).filter(Boolean).map((t) => t.replace(/\s+/g, " "));
  return [...new Set(tags)].join(", ");
}

function parseConfigurableVariations(str) {
  const map = new Map();
  if (isEmpty(str)) return map;

  const parts = String(str).split("|").map((x) => x.trim()).filter(Boolean);

  for (const part of parts) {
    // Find sku first
    const skuMatch = part.match(/(?:^|,)sku=([^,|]+)/);
    const sku = skuMatch ? skuMatch[1].trim() : "";
    if (!sku) continue;

    // Remove the sku=... segment so we can parse the rest
    let rest = part.replace(/(?:^|,)sku=[^,|]+/, "");

    // Parse key=value pairs where value may contain commas
    const attrs = {};
    let i = 0;

    while (i < rest.length) {
      // skip leading commas/spaces
      while (i < rest.length && (rest[i] === "," || rest[i] === " ")) i++;
      if (i >= rest.length) break;

      // read key up to '='
      const eq = rest.indexOf("=", i);
      if (eq === -1) break;

      const key = rest.slice(i, eq).trim();
      i = eq + 1;

      // read value until we hit ",<nextKey>=" pattern
      // nextKey: letters/numbers/underscore
      let next = i;
      while (next < rest.length) {
        if (rest[next] === ",") {
          const maybeKeyStart = next + 1;
          // look ahead for pattern ",something="
          const lookEq = rest.indexOf("=", maybeKeyStart);
          if (lookEq !== -1) {
            const candidateKey = rest.slice(maybeKeyStart, lookEq).trim();
            if (candidateKey && /^[a-zA-Z0-9_]+$/.test(candidateKey)) {
              break; // this comma starts a new key=value
            }
          }
        }
        next++;
      }

      const value = rest.slice(i, next).trim();
      if (key) attrs[key] = value;

      i = next; // continue after value (comma will be skipped at loop start)
    }

    map.set(sku, attrs);
  }

  return map;
}


function parseConfigurableLabels(str) {
  const out = [];
  if (isEmpty(str)) return out;

  const parts = String(str).split(",").map((x) => x.trim()).filter(Boolean);
  for (const p of parts) {
    const idx = p.indexOf("=");
    if (idx === -1) continue;
    const code = p.slice(0, idx).trim();
    const name = p.slice(idx + 1).trim();
    if (code && name) out.push({ code, name });
  }
  return out;
}

function getStatus(row) {
  const po = String(row.product_online ?? "").trim();
  if (po === "1" || po.toLowerCase() === "yes" || po.toLowerCase() === "true") return "active";
  return "draft";
}

function getPublished(row) {
  return getStatus(row) === "active" ? "TRUE" : "FALSE";
}

function pickFirstImage(row, logs) {
  const img = row.base_image || row.small_image || row.thumbnail_image || row.swatch_image || "";
  const raw = toStr(img).trim();
  if (!raw) return "";

  const fixed = normalizeImageUrl(raw);

  // keep a warning if it was not absolute originally (optional log only)
  if (raw && !isAbsoluteUrl(raw)) {
    logs.push(`WARN: Image was not absolute URL. Prefixed base domain. Raw="${raw}", Fixed="${fixed}" (SKU=${row.sku || "?"})`);
  }

  return fixed;
}

function makeEmptyRowObj(header) {
  const o = {};
  for (const c of header) o[c] = "";
  return o;
}

function rowToObject(headers, rowValues) {
  const obj = {};
  for (let i = 0; i < headers.length; i++) {
    const key = headers[i];
    obj[key] = rowValues[i + 1] ?? "";
  }
  return obj;
}

function hashKey(str) {
  return crypto.createHash("md5").update(String(str || "")).digest("hex");
}

async function readJsonLines(filePath) {
  const out = [];
  if (!fs.existsSync(filePath)) return out;

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const t = line.trim();
    if (!t) continue;
    out.push(JSON.parse(t));
  }
  return out;
}

function normalizeBoolean(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return "";
  if (s === "1" || s === "true" || s === "yes" || s === "y") return "TRUE";
  if (s === "0" || s === "false" || s === "no" || s === "n") return "FALSE";
  return "";
}

function normalizeListSingleLineText(v) {
  const s = String(v ?? "").trim();
  if (!s) return "";
  const arr = s
    .split(/[|,]/g)
    .map((x) => x.trim())
    .filter(Boolean);
  if (!arr.length) return "";
  return JSON.stringify([...new Set(arr)]);
}

/**
 * FIX #1 (EBUSY unlink on Windows)
 */
async function safeUnlink(filePath, logs, tries = 6) {
  if (!filePath) return;
  if (!fs.existsSync(filePath)) return;

  for (let i = 0; i < tries; i++) {
    try {
      fs.unlinkSync(filePath);
      return;
    } catch (err) {
      const code = err?.code;
      if (code === "EBUSY" || code === "EPERM") {
        await new Promise((r) => setTimeout(r, 50 * (i + 1)));
        continue;
      }
      logs?.push?.(`WARN: safeUnlink failed for ${filePath}: ${err?.message || String(err)}`);
      return;
    }
  }

  try {
    const renamed = `${filePath}.delete_${Date.now()}`;
    fs.renameSync(filePath, renamed);
    logs?.push?.(`WARN: Could not unlink due to lock. Renamed for later cleanup: ${path.basename(renamed)}`);
  } catch (e) {
    logs?.push?.(`WARN: Could not unlink or rename locked file ${filePath}: ${e?.message || String(e)}`);
  }
}

/**
 * FIX #2 (cleanup)
 */
function safeRmdirRecursive(dirPath, logs) {
  if (!dirPath) return;
  if (!fs.existsSync(dirPath)) return;
  try {
    fs.rmSync(dirPath, { recursive: true, force: true });
  } catch (e) {
    logs?.push?.(`WARN: Could not remove dir ${dirPath}: ${e?.message || String(e)}`);
  }
}

/** ---------- STREAMING CONVERTER (DISK SPOOLING) ---------- **/
async function convertBufferToShopifyXlsxStreaming(buffer) {
  console.log("========== START BUILDING SHOPIFY ROWS (STREAM) ==========");

  const logs = [];
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");

  ensureDir(OUTPUT_DIR);

  const spoolDir = path.join(OUTPUT_DIR, `spool_${stamp}`);
  const parentsDir = path.join(spoolDir, "parents");
  const childrenDir = path.join(spoolDir, "children");

  ensureDir(parentsDir);
  ensureDir(childrenDir);

  const outPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}.xlsx`);
  const logPath = path.join(OUTPUT_DIR, `shopify_products_${stamp}_logs.txt`);

  // Writer (streaming)
  const outWb = new ExcelJS.stream.xlsx.WorkbookWriter({
    filename: outPath,
    useStyles: false,
    useSharedStrings: true,
  });
  const outWs = outWb.addWorksheet("Shopify Products");

  // Reader (streaming) from buffer
  const inputStream = Readable.from(buffer);
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(inputStream, {
    worksheets: "emit",
    sharedStrings: "cache",
    hyperlinks: "ignore",
    styles: "ignore",
    entries: "emit",
  });

  let inputHeaders = null;
  let outputHeader = null;

  // Build product metafield columns strictly from client list (no scanning)
  const productMfColumns = PRODUCT_METAFIELD_SPECS.map((m) => {
    const key = toMetafieldKey(m.key);
    return `Metafield: ${MF_NAMESPACE}.${key} [${m.type}]`;
  });

  // Build variant metafield columns
  const variantMfColumns = VARIANT_METAFIELD_SPECS.map((m) => {
    const key = toMetafieldKey(m.key);
    return `Variant Metafield: ${MF_NAMESPACE}.${key} [${m.type}]`;
  });

  let rowNum = 1;
  let totalReadRows = 0;
  let totalOutputRows = 0;

  let parentCount = 0;
  let childCount = 0;
  let simpleCount = 0;

  const parentOrder = [];

  function writeRow(obj) {
    const values = outputHeader.map((h) => obj[h] ?? "");
    outWs.addRow(values).commit();
    totalOutputRows++;

    if (totalOutputRows % 5000 === 0) {
      console.log("Output rows written:", totalOutputRows);
    }
  }

  function attachProductMetafields(outRow, sourceRow) {
    const pt = toStr(sourceRow?.product_type).trim().toLowerCase();

    for (let i = 0; i < PRODUCT_METAFIELD_SPECS.length; i++) {
      const spec = PRODUCT_METAFIELD_SPECS[i];
      const colHeader = productMfColumns[i];

      // ✅ Only fill for configurable parents when requested
      if (spec.onlyForConfigurable && pt !== "configurable") continue;

      const rawVal = sourceRow[spec.source];
      if (isEmpty(rawVal)) continue;

      if (spec.type === "boolean") {
        const b = normalizeBoolean(rawVal);
        if (b) outRow[colHeader] = b;
        continue;
      }

      if (spec.type === "list.single_line_text_field") {
        const listVal = normalizeListSingleLineText(rawVal);
        if (listVal) outRow[colHeader] = listVal;
        continue;
      }

      outRow[colHeader] = toStr(rawVal);
    }
  }

  function attachVariantMetafields(outRow, sourceRow) {
    for (let i = 0; i < VARIANT_METAFIELD_SPECS.length; i++) {
      const spec = VARIANT_METAFIELD_SPECS[i];
      const colHeader = variantMfColumns[i];

      const rawVal =
        sourceRow?.[spec.source] ??
        (spec.fallbackSource ? sourceRow?.[spec.fallbackSource] : undefined);

      if (isEmpty(rawVal)) continue;

      // ✅ Support same formatting rules as product metafields
      if (spec.type === "boolean") {
        const b = normalizeBoolean(rawVal);
        if (b) outRow[colHeader] = b;
        continue;
      }

      if (spec.type === "list.single_line_text_field") {
        const listVal = normalizeListSingleLineText(rawVal);
        if (listVal) outRow[colHeader] = listVal;
        continue;
      }

      outRow[colHeader] = toStr(rawVal);
    }
  }

  function spoolParentRow(parentSku, rowObj) {
    const key = hashKey(parentSku);
    const parentPath = path.join(parentsDir, `${key}.json`);
    fs.writeFileSync(parentPath, JSON.stringify(rowObj), "utf8");
  }

  function spoolChildRow(parentSku, childObj) {
    const key = hashKey(parentSku);
    const childPath = path.join(childrenDir, `${key}.jsonl`);
    fs.appendFileSync(childPath, JSON.stringify(childObj) + "\n", "utf8");
  }

  console.log("Streaming workbook reading started...");

  for await (const worksheetReader of reader) {
    console.log("Worksheet found:", worksheetReader.name);

    let headerRowSeen = false;

    for await (const row of worksheetReader) {
      totalReadRows++;

      if (totalReadRows % 10000 === 0) {
        console.log("Input rows read:", totalReadRows);
        console.log("Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));
      }

      if (!headerRowSeen) {
        headerRowSeen = true;

        const rawHeaders = row.values.slice(1).map((h) => toStr(h).trim());
        inputHeaders = rawHeaders.map((h) => slugify(h).replace(/-/g, "_"));

        console.log("Header parsed. Total columns:", inputHeaders.length);

        // Output header = base Shopify + product metafields + variant metafields
        outputHeader = [...SHOPIFY_COLUMNS, ...productMfColumns, ...variantMfColumns];

        outWs.columns = outputHeader.map((h) => ({ header: h, key: h }));
        outWs.addRow(outputHeader).commit();

        console.log("Output header written. Total output columns:", outputHeader.length);
        continue;
      }

      const rowObj = rowToObject(inputHeaders, row.values);

      const pt = toStr(rowObj.product_type).trim().toLowerCase();
      const sku = toStr(rowObj.sku).trim();
      const parentSkuRaw = toStr(rowObj.parent_sku).trim();
      const parentSku = normalizeParentSku(parentSkuRaw); // ✅ FIX: resolved parent sku

      if (!sku && !pt) continue;

      // SIMPLE standalone -> write immediately
      if (pt === "simple" && isEmpty(parentSkuRaw)) {
        simpleCount++;

        if (totalReadRows % 5000 === 0) {
          console.log("Processing SIMPLE products... current input row:", totalReadRows);
        }

        const handle = toStr(rowObj.url_key).trim() ? slugify(rowObj.url_key) : slugify(rowObj.name || rowObj.sku);

        const specialPrice = safeNumber(rowObj.special_price);
        const msrpPrice = safeNumber(rowObj.msrp_price);
        const qty = safeNumber(rowObj.qty);

        const out = makeEmptyRowObj(outputHeader);
        out["Row #"] = rowNum;
        out["Top Row"] = 1;

        out["Handle"] = handle;
        out["Title"] = toStr(rowObj.name);
        out["Body HTML"] = toStr(rowObj.description || rowObj.short_description);

        out["Vendor"] = toStr(rowObj.manufacturer);

        out["Type"] = "simple";

        // ✅ FIX (2): Gift Card FALSE by default
        out["Gift Card"] = "FALSE";

        out["Status"] = getStatus(rowObj);
        out["Published"] = getPublished(rowObj);
        out["Created At"] = toStr(rowObj.created_at);
        out["Updated At"] = toStr(rowObj.updated_at);
        out["Tags"] = asTags(rowObj.categories, rowObj.categories_store_name);

        out["Metafield: title_tag [string]"] = toStr(rowObj.meta_title);
        out["Metafield: description_tag [string]"] = toStr(rowObj.meta_description);

        const img = pickFirstImage(rowObj, logs);
        if (img) {
          out["Image Src"] = img;
          out["Image Position"] = 1;
        }

        out["Option1 Name"] = "Title";
        out["Option1 Value"] = "Default Title";

        out["Variant SKU"] = sku;

        out["Variant Price"] = specialPrice > 0 ? String(specialPrice) : "";
        out["Variant Compare At Price"] = msrpPrice > 0 ? String(msrpPrice) : "";

        out["Variant Inventory Qty"] = String(qty || 0);
        out["Inventory Available: Shop location"] = String(qty || 0);

        const w = safeNumber(rowObj.weight);
        if (w > 0) {
          out["Variant Weight"] = String(w);
          out["Variant Weight Unit"] = "lb";
        }

        out["Variant Inventory Tracker"] = "shopify";
        out["Variant Inventory Policy"] = "deny";
        out["Variant Requires Shipping"] = "TRUE";
        out["Variant Taxable"] = "TRUE";

        attachProductMetafields(out, rowObj);
        attachVariantMetafields(out, rowObj);

        writeRow(out);
        rowNum++;
        continue;
      }

      // CONFIGURABLE parent -> spool
      if (pt === "configurable") {
        parentCount++;

        if (totalReadRows % 2000 === 0) {
          console.log("Processing CONFIGURABLE parents... current input row:", totalReadRows);
        }

        spoolParentRow(sku, rowObj);
        parentOrder.push(sku);
        continue;
      }

      // CHILD variant -> spool
      if (pt === "simple" && !isEmpty(parentSkuRaw)) {
        childCount++;

        // ✅ log when we had to resolve comma-separated parent_sku
        if (parentSkuRaw && parentSku && parentSkuRaw !== parentSku) {
          logs.push(`INFO: parent_sku had multiple values. Using resolved parent_sku="${parentSku}" (raw="${parentSkuRaw}") for child SKU=${sku}`);
        }

        spoolChildRow(parentSku, rowObj);

        if (childCount % 5000 === 0) {
          console.log("Children spooled:", childCount);
        }
        continue;
      }

      logs.push(`WARN: Unhandled product_type="${pt}" for SKU=${sku}`);
    }

    break;
  }

  // PHASE 2: build configurable products from spooled data
  console.log("========== PHASE 2: BUILD CONFIGURABLE PRODUCTS ==========");
  console.log("Parents found:", parentCount);
  console.log("Children spooled:", childCount);

  for (let p = 0; p < parentOrder.length; p++) {
    const parentSku = parentOrder[p];
    const key = hashKey(parentSku);
    const parentPath = path.join(parentsDir, `${key}.json`);
    const childPath = path.join(childrenDir, `${key}.jsonl`);

    if (p % 500 === 0) {
      console.log("Configurable build progress:", p, "/", parentOrder.length);
    }

    if (!fs.existsSync(parentPath)) continue;

    const parent = JSON.parse(fs.readFileSync(parentPath, "utf8"));
    const children = await readJsonLines(childPath);

    if (!children.length) {
      logs.push(`INFO: Configurable parent has NO children rows. SKU=${parentSku} (will create Default Title variant)`);

      const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
      const handle = handleBase || slugify(parentSku || `product-${rowNum}`);

      const specialPrice = safeNumber(parent.special_price);
      const msrpPrice = safeNumber(parent.msrp_price);
      const qty = safeNumber(parent.qty);

      const out = makeEmptyRowObj(outputHeader);
      out["Row #"] = rowNum;
      out["Top Row"] = 1;
      out["Handle"] = handle;

      out["Title"] = toStr(parent.name);
      out["Body HTML"] = toStr(parent.description || parent.short_description);

      out["Vendor"] = toStr(parent.manufacturer);

      out["Type"] = "configurable";

      // ✅ FIX (2): Gift Card FALSE by default
      out["Gift Card"] = "FALSE";

      out["Status"] = getStatus(parent);
      out["Published"] = getPublished(parent);
      out["Created At"] = toStr(parent.created_at);
      out["Updated At"] = toStr(parent.updated_at);
      out["Tags"] = asTags(parent.categories, parent.categories_store_name);

      out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
      out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

      const img = pickFirstImage(parent, logs);
      if (img) {
        out["Image Src"] = img;
        out["Image Position"] = 1;
      }

      out["Option1 Name"] = "Title";
      out["Option1 Value"] = "Default Title";

      out["Variant SKU"] = parentSku;

      out["Variant Price"] = specialPrice > 0 ? String(specialPrice) : "";
      out["Variant Compare At Price"] = msrpPrice > 0 ? String(msrpPrice) : "";

      out["Variant Inventory Qty"] = String(qty || 0);
      out["Inventory Available: Shop location"] = String(qty || 0);

      const w = safeNumber(parent.weight);
      if (w > 0) {
        out["Variant Weight"] = String(w);
        out["Variant Weight Unit"] = "lb";
      }

      out["Variant Inventory Tracker"] = "shopify";
      out["Variant Inventory Policy"] = "deny";
      out["Variant Requires Shipping"] = "TRUE";
      out["Variant Taxable"] = "TRUE";

      attachProductMetafields(out, parent);
      attachVariantMetafields(out, parent);

      writeRow(out);
      rowNum++;
    } else {
      const handleBase = toStr(parent.url_key).trim() ? slugify(parent.url_key) : slugify(parent.name || parentSku);
      const handle = handleBase || slugify(parentSku || `product-${rowNum}`);

      const labelList = parseConfigurableLabels(parent.configurable_variation_labels);
      const variationMap = parseConfigurableVariations(parent.configurable_variations);

      let optionDefs = labelList;
      if (!optionDefs.length && variationMap.size) {
        const first = variationMap.values().next().value || {};
        optionDefs = Object.keys(first).slice(0, 3).map((code) => ({ code, name: code }));
        logs.push(`WARN: No configurable_variation_labels for parent SKU=${parentSku}. Inferred options from variations keys.`);
      }

      children.sort((a, b) => toStr(a.sku).localeCompare(toStr(b.sku)));

      // ✅ FIX: keep Image Src populated for variant images too.
      // ALSO ensure FIRST variant image is included (even when top row uses parent image).
      const parentImg = pickFirstImage(parent, logs);
      let nextImagePos = 0;

      for (let i = 0; i < children.length; i++) {
        const child = children[i];
        const childSku = toStr(child.sku).trim();

        const out = makeEmptyRowObj(outputHeader);
        out["Row #"] = rowNum;
        out["Handle"] = handle;
        out["Variant Position"] = String(i + 1);

        const isTop = i === 0;
        out["Top Row"] = isTop ? 1 : "";

        if (isTop) {
          out["Title"] = toStr(parent.name);
          out["Body HTML"] = toStr(parent.description || parent.short_description);

          out["Vendor"] = toStr(parent.manufacturer);
          out["Type"] = "configurable";
          out["Gift Card"] = "FALSE";

          out["Status"] = getStatus(parent);
          out["Published"] = getPublished(parent);
          out["Created At"] = toStr(parent.created_at);
          out["Updated At"] = toStr(parent.updated_at);
          out["Tags"] = asTags(parent.categories, parent.categories_store_name);

          out["Metafield: title_tag [string]"] = toStr(parent.meta_title);
          out["Metafield: description_tag [string]"] = toStr(parent.meta_description);

          // parent image stays on top row (Image Position = 1)
          if (parentImg) {
            nextImagePos = 1;
            out["Image Src"] = parentImg;
            out["Image Position"] = nextImagePos;
          }

          attachProductMetafields(out, parent);
        }

        const specialPrice = safeNumber(child.special_price);
        const msrpPrice = safeNumber(child.msrp_price);
        const qty = safeNumber(child.qty);

        out["Variant SKU"] = childSku;
        out["Variant Price"] = specialPrice > 0 ? String(specialPrice) : "";
        out["Variant Compare At Price"] = msrpPrice > 0 ? String(msrpPrice) : "";
        out["Variant Inventory Qty"] = String(qty || 0);
        out["Inventory Available: Shop location"] = String(qty || 0);

        const vImg = pickFirstImage(child, logs);
        if (vImg) out["Variant Image"] = vImg;

        // Non-top variants: put variant image into Image Src on SAME row (existing behavior)
        if (!isTop && vImg) {
          nextImagePos = nextImagePos > 0 ? nextImagePos + 1 : 1;
          out["Image Src"] = vImg;
          out["Image Position"] = nextImagePos;
        }

        // If parent has NO image, allow top row to use first variant image (existing behavior)
        if (isTop && isEmpty(out["Image Src"]) && vImg) {
          nextImagePos = 1;
          out["Image Src"] = vImg;
          out["Image Position"] = nextImagePos;
        }

        const w = safeNumber(child.weight);
        if (w > 0) {
          out["Variant Weight"] = String(w);
          out["Variant Weight Unit"] = "lb";
        }

        const attrs = variationMap.get(childSku) || null;
        if (!attrs && optionDefs.length) {
          logs.push(`WARN: Could not find option values for child SKU=${childSku} under parent SKU=${parentSku}.`);
        }

        if (optionDefs.length) {
          for (let oi = 0; oi < 3; oi++) {
            const def = optionDefs[oi];
            if (!def) break;
            out[`Option${oi + 1} Name`] = def.name;
            out[`Option${oi + 1} Value`] = !isEmpty(attrs?.[def.code]) ? toStr(attrs[def.code]) : "";
          }
        } else {
          out["Option1 Name"] = "Title";
          out["Option1 Value"] = "Default Title";
        }

        out["Variant Inventory Tracker"] = "shopify";
        out["Variant Inventory Policy"] = "deny";
        out["Variant Requires Shipping"] = "TRUE";
        out["Variant Taxable"] = "TRUE";

        attachVariantMetafields(out, child);

        // ✅ write variant row
        writeRow(out);
        rowNum++;

        // ✅ NEW: If TOP variant has its own image AND parent image exists,
        // add an extra IMAGE-ONLY row so Image Src list includes first variant image too.
        if (isTop && parentImg && vImg && vImg !== parentImg) {
          nextImagePos = nextImagePos > 0 ? nextImagePos + 1 : 1;

          const imgOnly = makeEmptyRowObj(outputHeader);
          imgOnly["Row #"] = rowNum;
          imgOnly["Handle"] = handle;
          imgOnly["Image Src"] = vImg;
          imgOnly["Image Position"] = nextImagePos;

          writeRow(imgOnly);
          rowNum++;
        }
      }
    }

    await safeUnlink(childPath, logs);
  }

  // leftover children files are orphans -> log + delete
  const remainingChildFiles = fs.existsSync(childrenDir) ? fs.readdirSync(childrenDir) : [];
  for (const f of remainingChildFiles) {
    const fp = path.join(childrenDir, f);
    try {
      const orphanChildren = await readJsonLines(fp);
      logs.push(`WARN: Orphan children file ${f} has ${orphanChildren.length} rows (parent missing in sheet).`);
    } catch (e) {
      logs.push(`WARN: Could not read orphan children file ${f}: ${e?.message || String(e)}`);
    } finally {
      await safeUnlink(fp, logs);
    }
  }

  console.log("Finishing output workbook...");
  await outWb.commit();

  fs.writeFileSync(logPath, logs.join("\n"), "utf8");

  safeRmdirRecursive(spoolDir, logs);

  console.log("========== STREAM CONVERSION COMPLETE ==========");
  console.log("Total input rows read:", totalReadRows);
  console.log("Total output rows written:", totalOutputRows);
  console.log("Simple written:", simpleCount);
  console.log("Parents spooled:", parentCount);
  console.log("Children spooled:", childCount);
  console.log("Output file:", outPath);
  console.log("Logs file:", logPath);

  return { outPath, logPath, totalReadRows, totalOutputRows, logsCount: logs.length };
}

/** ---------- EXPRESS HANDLER ---------- **/
export async function convertToShopifySheet(req, res) {
  try {
    if (!req?.file?.buffer) {
      return res.status(400).json({
        status: false,
        message: "Missing file buffer. Please upload XLSX as multipart/form-data (field name: file).",
      });
    }

    console.log("First 20 bytes:", req.file.buffer.slice(0, 20).toString("hex"));
    console.log("First 50 chars:", req.file.buffer.slice(0, 50).toString("utf8"));

    console.log("========== FILE RECEIVED ==========");
    console.log("Buffer size (MB):", (req.file.buffer.length / 1024 / 1024).toFixed(2));
    console.log("Start Time:", new Date().toISOString());

    const result = await convertBufferToShopifyXlsxStreaming(req.file.buffer);

    console.log("End Time:", new Date().toISOString());
    console.log("Final Memory Usage (MB):", (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2));

    return res.json({
      status: true,
      message: "Converted to Shopify formatted sheet (streaming + spooling).",
      result: {
        shopifySheetPath: result.outPath,
        logsPath: result.logPath,
        stats: {
          inputRowsRead: result.totalReadRows,
          outputRowsWritten: result.totalOutputRows,
          logsCount: result.logsCount,
        },
      },
    });
  } catch (err) {
    console.log("ERROR:", err);
    return res.status(500).json({
      status: false,
      message: "Internal server error.",
      result: { error: err?.message || String(err) },
    });
  }
}

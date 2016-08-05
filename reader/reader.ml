(*
 * Copyright (C) Citrix Systems Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

open Crc
open Cstruct
open Cmdliner

exception Invalid_header_string
exception Invalid_checksum
exception Invalid_payload
exception No_update
exception Read_error
exception Unknown_datasource_type

(* Field sizes. *)
let default_header = "DATASOURCES"

let header_bytes = String.length default_header

let data_crc_bytes = 4

let metadata_crc_bytes = 4

let datasource_count_bytes = 4

let timestamp_bytes = 8

let datasource_value_bytes = 8

let metadata_length_bytes = 4

let get_total_bytes datasource_count metadata_length =
	header_bytes +
	data_crc_bytes +
	metadata_crc_bytes +
	datasource_count_bytes +
	timestamp_bytes +
	(datasource_value_bytes * datasource_count) +
	metadata_length_bytes +
	metadata_length

(* Field start points. *)
let header_start = 0

let data_crc_start = header_start + header_bytes

let metadata_crc_start = data_crc_start + data_crc_bytes

let datasource_count_start = metadata_crc_start + metadata_crc_bytes

let timestamp_start = datasource_count_start + datasource_count_bytes

let datasource_value_start = timestamp_start + timestamp_bytes

let get_metadata_length_start datasource_count =
	datasource_value_start + (datasource_count * datasource_value_bytes)

let get_metadata_start datasource_count =
	(get_metadata_length_start datasource_count) + metadata_length_bytes

type ds_owner = VM of string | Host | SR of string

type ds_type = Absolute | Gauge | Derive

type ds_value_type = VT_Float of float | VT_Int64 of int64 | VT_Unknown

type ds = {
	ds_name: string;
	ds_description: string;
	ds_value: ds_value_type;
	ds_type: ds_type;
	ds_default: bool;
	ds_min: float;
	ds_max: float;
	ds_units: string;
	ds_pdp_transform_function: float -> float;
}
let ds_make ~name ~description ~value ~ty ~default ?(min=0.00) ?(max=1000.00) ?(units="") ?(transform=(fun x -> x)) () = {
	ds_name=name;
	ds_description=description;
	ds_value=value;
	ds_type=ty;
	ds_default=default;
	ds_min=min;
	ds_max=max;
	ds_units=units;
	ds_pdp_transform_function=transform;
}

let readRRDFile filename =
	let fd = Unix.openfile filename [Unix.O_RDONLY] 0o400 in
	if Unix.lseek fd 0 Unix.SEEK_SET <> 0 then
		raise Read_error;
	let mapping = Bigarray.(Array1.map_file fd char c_layout false (-1)) in
	Unix.close fd;
	Cstruct.of_bigarray mapping

(* Reading fields from cstruct. *)
module Read = struct
	let header cs =
		let header = Bytes.create header_bytes in
		Cstruct.blit_to_string cs header_start header 0 header_bytes;
		header

	let data_crc cs =
		Cstruct.BE.get_uint32 cs data_crc_start

	let metadata_crc cs =
		Cstruct.BE.get_uint32 cs metadata_crc_start

	let datasource_count cs =
		Int32.to_int (Cstruct.BE.get_uint32 cs datasource_count_start)

	let timestamp cs =
		Cstruct.BE.get_uint64 cs timestamp_start

	let datasource_values cs cached_datasources =
		let rec aux start acc = function
			| [] -> acc
			| (owner, cached_datasource) :: rest -> begin
				(* Replace the cached datasource's value with the value read
				 * from the cstruct. *)
				let value = match cached_datasource.ds_value with
				| VT_Float _ ->
					VT_Float (Int64.float_of_bits (Cstruct.BE.get_uint64 cs start))
				| VT_Int64 _ ->
					VT_Int64 (Cstruct.BE.get_uint64 cs start)
				| VT_Unknown -> raise Unknown_datasource_type
				in
				aux (start + datasource_value_bytes)
					((owner, {cached_datasource with ds_value = value}) :: acc)
					rest
			end
		in
		List.rev (aux datasource_value_start [] cached_datasources)

	let metadata_length cs datasource_count =
		Int32.to_int (Cstruct.BE.get_uint32 cs (get_metadata_length_start datasource_count))

	let metadata cs datasource_count metadata_length =
		let metadata = Bytes.create metadata_length in
		Cstruct.blit_to_string
			cs (get_metadata_start datasource_count)
			metadata 0 metadata_length;
		metadata
end

(* Extracting the dictionary out of the RPC type. *)
module Rrd_rpc = struct
	let dict_of_rpc ~(rpc : Rpc.t) : (string * Rpc.t) list =
		match rpc with Rpc.Dict d -> d | _ -> raise Invalid_payload

	(* A helper function for extracting the enum/list out of the RPC type. *)
	let list_of_rpc ~(rpc : Rpc.t) : Rpc.t list =
		match rpc with Rpc.Enum l -> l | _ -> raise Invalid_payload

	(* [assoc_opt ~key ~default l] gets string value associated with [key] in
	 * [l], returning [default] if no mapping is found. *)
	let assoc_opt ~(key : string) ~(default : string) (l : (string * Rpc.t) list) : string =
		try Rpc.string_of_rpc (List.assoc key l) with
		| Not_found -> default
		| e -> raise e

	(* Converts string to the corresponding datasource type. *)
	let ds_ty_of_string (s : string) : ds_type =
		match String.lowercase s with
		| "gauge" -> Gauge
		| "absolute" -> Absolute
		| "derive" -> Derive
		| _ -> raise Invalid_payload

		(* Converts a string to value of datasource owner type. *)
	let owner_of_string (s : string) : ds_owner =
		match Stringext.split ~on:' ' (String.lowercase s) with
		| ["host"] -> Host
		| ["vm"; uuid] -> VM uuid
		| ["sr"; uuid] -> SR uuid
		| _ -> raise Invalid_payload
end

let default_value_of_string (s : string) : ds_value_type =
	match s with
	| "float" -> VT_Float 0.0
	| "int64" -> VT_Int64 0L
	| _ -> raise Invalid_payload

(* WARNING! This creates datasources from datasource metadata, hence the
 * values will be meaningless. The types however, will be correct. *)
let uninitialised_ds_of_rpc ((name, rpc) : (string * Rpc.t)) : (ds_owner * ds) =
	let open Rpc in
	let kvs = Rrd_rpc.dict_of_rpc ~rpc in
	let description = Rrd_rpc.assoc_opt ~key:"description" ~default:"" kvs in
	let units = Rrd_rpc.assoc_opt ~key:"units" ~default:"" kvs in
	let ty =
		Rrd_rpc.ds_ty_of_string
		(Rrd_rpc.assoc_opt ~key:"type" ~default:"absolute" kvs)
	in
	let value =
		default_value_of_string (Rpc.string_of_rpc (List.assoc "value_type" kvs)) in
	let min =
		float_of_string (Rrd_rpc.assoc_opt ~key:"min" ~default:"-infinity" kvs) in
	let max =
		float_of_string (Rrd_rpc.assoc_opt ~key:"max" ~default:"infinity" kvs) in
	let owner =
		Rrd_rpc.owner_of_string
		(Rrd_rpc.assoc_opt ~key:"owner" ~default:"host" kvs) in
	let default =
		bool_of_string (Rrd_rpc.assoc_opt ~key:"default" ~default:"false" kvs) in
	let ds =
		ds_make ~name ~description ~units ~ty ~value ~min ~max ~default () in
	owner, ds

let parse_metadata metadata =
	try
		let open Rpc in
		let rpc = Jsonrpc.of_string metadata in
		let kvs = Rrd_rpc.dict_of_rpc ~rpc in
		let datasource_rpcs = Rrd_rpc.dict_of_rpc (List.assoc "datasources" kvs) in
		List.map uninitialised_ds_of_rpc datasource_rpcs
	with _ -> raise Invalid_payload
	

let make_payload_reader () =
	let last_data_crc = ref 0l in
	let last_metadata_crc = ref 0l in
	let cached_datasources : (ds_owner * ds) list ref = ref [] in
	(fun cs ->
		(* Check the header string is present and correct. *)
		let header = Read.header cs in
		if not (header = default_header) then
			raise Invalid_header_string;
			(* Check that the data CRC has changed. Since the CRC'd data
			 * includes the timestamp, this should change with every update. *)
		let data_crc = Read.data_crc cs in
		if data_crc = !last_data_crc then raise No_update;
		let metadata_crc = Read.metadata_crc cs in
		let datasource_count = Read.datasource_count cs in
		let timestamp = Read.timestamp cs in
		(* Check the data crc is correct. *)
		let data_crc_calculated =
			Crc32.cstruct
				(Cstruct.sub cs timestamp_start
					(timestamp_bytes + datasource_count * datasource_value_bytes))
		in
		if not (data_crc = data_crc_calculated)
		then raise Invalid_checksum
		else last_data_crc := data_crc;
		(* Read the datasource values. *)
		let datasources =
			if metadata_crc = !last_metadata_crc then begin
				(* Metadata hasn't changed, so just read the datasources values. *)
				Read.datasource_values cs !cached_datasources
			end else begin
				(* Metadata has changed - we need to read it to find the types of the
				 * datasources, then go back and read the values themselves. *)
				let metadata_length = Read.metadata_length cs datasource_count in
				let metadata = Read.metadata cs datasource_count metadata_length in
				(* Check the metadata checksum is correct. *)
				if not (metadata_crc = Crc32.string metadata 0 metadata_length)
				then raise Invalid_checksum;
				(* If all is OK, cache the metadata checksum and read the values
				 * based on this new metadata. *)
				last_metadata_crc := metadata_crc;
				Read.datasource_values cs (parse_metadata metadata)
			end
		in
		cached_datasources := datasources;
		{
			timestamp = timestamp;
			datasources = datasources;
		})

let () =
	let filename = "rrdPlugin1.rrd" in
	let cs = readRRDFile filename in
	let reader = make_payload_reader () in
	let read_payload () = reader cs in
	read_payload;


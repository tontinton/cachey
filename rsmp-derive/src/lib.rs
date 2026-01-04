use std::collections::HashSet;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, FnArg, ItemTrait, LitInt, Pat, ReturnType, TraitItem, Type,
    parse_macro_input,
};

type FieldIndex = u16;

#[proc_macro_derive(Args, attributes(field))]
pub fn derive_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match &input.data {
        Data::Struct(_) => impl_args_struct(&input),
        Data::Enum(_) => impl_args_enum(&input),
        _ => Err(syn::Error::new_spanned(
            &input,
            "only structs and enums supported",
        )),
    }
    .unwrap_or_else(|e| e.to_compile_error())
    .into()
}

struct FieldInfo {
    name: syn::Ident,
    idx: FieldIndex,
    ty: Type,
    is_option: bool,
}

fn impl_args_struct(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    let data = match &input.data {
        Data::Struct(data) => data,
        _ => return Err(syn::Error::new_spanned(input, "only structs supported")),
    };

    if matches!(&data.fields, Fields::Unit) {
        return Ok(quote! {
            impl rsmp::Args for #name {
                fn encode_args(&self) -> Vec<u8> {
                    (0 as rsmp::FieldIndex).to_be_bytes().to_vec()
                }

                fn decode_args(_data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                    Ok(Self)
                }
            }

            impl rsmp::Encode for #name {
                fn encode(&self, buf: &mut Vec<u8>) {
                    buf.extend_from_slice(&rsmp::Args::encode_args(self));
                }
                fn wire_type(&self) -> rsmp::WireType {
                    rsmp::WireType::Bytes
                }
            }

            impl rsmp::Decode<'_> for #name {
                fn decode(_wire_type: rsmp::WireType, data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                    rsmp::Args::decode_args(data)
                }
            }

            impl std::default::Default for #name {
                fn default() -> Self {
                    Self
                }
            }
        });
    }

    let fields = match &data.fields {
        Fields::Named(fields) => &fields.named,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "only named fields or unit structs supported",
            ));
        }
    };

    let mut field_info: Vec<FieldInfo> = Vec::new();

    for field in fields {
        let field_name = field.ident.clone().unwrap();
        let field_type = field.ty.clone();
        let is_option = is_option_type(&field_type);

        let mut field_idx: Option<FieldIndex> = None;

        for attr in &field.attrs {
            if attr.path().is_ident("field") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("idx") {
                        let value: LitInt = meta.value()?.parse()?;
                        field_idx = Some(value.base10_parse()?);
                    }
                    Ok(())
                })?;
            }
        }

        let idx = field_idx
            .ok_or_else(|| syn::Error::new_spanned(field, "missing #[field(idx = N)] attribute"))?;

        field_info.push(FieldInfo {
            name: field_name,
            idx,
            ty: field_type,
            is_option,
        });
    }

    let mut seen_indices = HashSet::new();
    for f in &field_info {
        if !seen_indices.insert(f.idx) {
            return Err(syn::Error::new_spanned(
                input,
                format!("duplicate field index: {}", f.idx),
            ));
        }
    }

    let encode_fields = field_info.iter().map(|f| {
        let name = &f.name;
        let idx = f.idx;

        if f.is_option {
            quote! {
                match &self.#name {
                    Some(v) => {
                        let mut field_data = Vec::new();
                        rsmp::Encode::encode(v, &mut field_data);
                        rsmp::write_field(
                            &mut buf,
                            #idx as rsmp::FieldIndex,
                            rsmp::Encode::wire_type(v),
                            &field_data,
                        );
                    }
                    None => {
                        rsmp::write_field(
                            &mut buf,
                            #idx as rsmp::FieldIndex,
                            rsmp::WireType::None,
                            &[],
                        );
                    }
                }
            }
        } else {
            quote! {
                {
                    let mut field_data = Vec::new();
                    rsmp::Encode::encode(&self.#name, &mut field_data);
                    rsmp::write_field(
                        &mut buf,
                        #idx as rsmp::FieldIndex,
                        rsmp::Encode::wire_type(&self.#name),
                        &field_data,
                    );
                }
            }
        }
    });

    let decode_vars = field_info.iter().map(|f| {
        let var_name = format_ident!("field_{}", f.name);
        let ty = &f.ty;
        if f.is_option {
            quote! { let mut #var_name: #ty = None; }
        } else {
            quote! { let mut #var_name: Option<_> = None; }
        }
    });

    let decode_matches = field_info.iter().map(|f| {
        let var_name = format_ident!("field_{}", f.name);
        let idx = f.idx;

        if f.is_option {
            let inner_ty = extract_option_inner(&f.ty);
            quote! {
                #idx => {
                    if __wire_type != rsmp::WireType::None {
                        #var_name = Some(
                            <#inner_ty as rsmp::Decode<'_>>::decode(__wire_type, __bytes)?
                        );
                    }
                }
            }
        } else {
            let ty = &f.ty;
            quote! {
                #idx => {
                    #var_name = Some(
                        <#ty as rsmp::Decode<'_>>::decode(__wire_type, __bytes)?
                    );
                }
            }
        }
    });

    let build_fields = field_info.iter().map(|f| {
        let name = &f.name;
        let var_name = format_ident!("field_{}", f.name);
        let idx = f.idx;

        if f.is_option {
            quote! { #name: #var_name }
        } else {
            quote! {
                #name: #var_name.ok_or(rsmp::ProtocolError::MissingField(#idx as rsmp::FieldIndex))?
            }
        }
    });

    let field_count = field_info.len();

    Ok(quote! {
        impl rsmp::Args for #name {
            fn encode_args(&self) -> Vec<u8> {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(#field_count as rsmp::FieldIndex).to_be_bytes());
                #(#encode_fields)*
                buf
            }

            fn decode_args(data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                const FIELD_INDEX_SIZE: usize = std::mem::size_of::<rsmp::FieldIndex>();
                if data.len() < FIELD_INDEX_SIZE {
                    return Err(rsmp::ProtocolError::UnexpectedEof);
                }

                let field_count = rsmp::FieldIndex::from_be_bytes(
                    data[..FIELD_INDEX_SIZE].try_into().unwrap()
                );
                let mut offset = FIELD_INDEX_SIZE;

                #(#decode_vars)*

                for _ in 0..field_count {
                    let (__idx, __wire_type, __bytes, __consumed) =
                        rsmp::read_field(&data[offset..])?;
                    offset += __consumed;

                    match __idx {
                        #(#decode_matches)*
                        _ => {}
                    }
                }

                Ok(Self {
                    #(#build_fields),*
                })
            }
        }

        impl rsmp::Encode for #name {
            fn encode(&self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&rsmp::Args::encode_args(self));
            }
            fn wire_type(&self) -> rsmp::WireType {
                rsmp::WireType::Bytes
            }
        }

        impl rsmp::Decode<'_> for #name {
            fn decode(_wire_type: rsmp::WireType, data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                rsmp::Args::decode_args(data)
            }
        }
    })
}

fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Option";
    }
    false
}

fn extract_option_inner(ty: &Type) -> TokenStream2 {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return quote! { #inner };
    }
    quote! { () }
}

struct VariantInfo {
    name: syn::Ident,
    idx: FieldIndex,
    inner_ty: Type,
}

fn impl_args_enum(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => return Err(syn::Error::new_spanned(input, "expected enum")),
    };

    let mut variant_info: Vec<VariantInfo> = Vec::new();

    for variant in variants {
        let inner_ty = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                fields.unnamed.first().unwrap().ty.clone()
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "enum variants must have exactly one unnamed field",
                ));
            }
        };

        let mut variant_idx: Option<FieldIndex> = None;

        for attr in &variant.attrs {
            if attr.path().is_ident("field") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("idx") {
                        let value: LitInt = meta.value()?.parse()?;
                        variant_idx = Some(value.base10_parse()?);
                    }
                    Ok(())
                })?;
            }
        }

        let idx = variant_idx.ok_or_else(|| {
            syn::Error::new_spanned(variant, "missing #[field(idx = N)] attribute")
        })?;

        variant_info.push(VariantInfo {
            name: variant.ident.clone(),
            idx,
            inner_ty,
        });
    }

    let mut seen_indices = HashSet::new();
    for v in &variant_info {
        if !seen_indices.insert(v.idx) {
            return Err(syn::Error::new_spanned(
                input,
                format!("duplicate variant index: {}", v.idx),
            ));
        }
    }

    let encode_arms = variant_info.iter().map(|v| {
        let vname = &v.name;
        let idx = v.idx;
        quote! {
            Self::#vname(inner) => {
                buf.extend_from_slice(&(#idx as rsmp::FieldIndex).to_be_bytes());
                buf.extend_from_slice(&rsmp::Args::encode_args(inner));
            }
        }
    });

    let decode_arms = variant_info.iter().map(|v| {
        let vname = &v.name;
        let idx = v.idx;
        let ty = &v.inner_ty;
        quote! {
            #idx => Ok(Self::#vname(<#ty as rsmp::Args>::decode_args(&data[FIELD_INDEX_SIZE..])?)),
        }
    });

    Ok(quote! {
        impl rsmp::Args for #name {
            fn encode_args(&self) -> Vec<u8> {
                let mut buf = Vec::new();
                match self {
                    #(#encode_arms)*
                }
                buf
            }

            fn decode_args(data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                const FIELD_INDEX_SIZE: usize = std::mem::size_of::<rsmp::FieldIndex>();
                if data.len() < FIELD_INDEX_SIZE {
                    return Err(rsmp::ProtocolError::UnexpectedEof);
                }
                let variant_id = rsmp::FieldIndex::from_be_bytes(
                    data[..FIELD_INDEX_SIZE].try_into().unwrap()
                );
                match variant_id {
                    #(#decode_arms)*
                    _ => Err(rsmp::ProtocolError::UnknownVariant(variant_id)),
                }
            }
        }

        impl rsmp::Encode for #name {
            fn encode(&self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&rsmp::Args::encode_args(self));
            }
            fn wire_type(&self) -> rsmp::WireType {
                rsmp::WireType::Bytes
            }
        }

        impl rsmp::Decode<'_> for #name {
            fn decode(_wire_type: rsmp::WireType, data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                rsmp::Args::decode_args(data)
            }
        }
    })
}

struct ArgInfo {
    name: syn::Ident,
    ty: Type,
    is_option: bool,
}

struct MethodInfo {
    name: syn::Ident,
    idx: u16,
    args: Vec<ArgInfo>,
    has_request_stream: bool,
    has_response_stream: bool,
    has_response_data: bool,
    response_ty: Option<Type>,
}

fn is_stream_marker_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Stream";
    }
    false
}

fn extract_tuple_types(ty: &Type) -> Option<Vec<&Type>> {
    if let Type::Tuple(tuple) = ty {
        return Some(tuple.elems.iter().collect());
    }
    None
}

fn extract_base_response_type(ty: &Type) -> Option<Type> {
    if is_stream_marker_type(ty) {
        return None;
    }
    if let Some(types) = extract_tuple_types(ty) {
        let non_stream: Vec<_> = types
            .into_iter()
            .filter(|t| !is_stream_marker_type(t))
            .collect();
        if non_stream.is_empty() {
            return None;
        }
        if non_stream.len() == 1 {
            return Some(non_stream[0].clone());
        }
    }
    if is_unit_type(ty) {
        return None;
    }
    Some(ty.clone())
}

fn is_unit_type(ty: &Type) -> bool {
    if let Type::Tuple(tuple) = ty {
        return tuple.elems.is_empty();
    }
    false
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

struct ServiceAttr {
    error_ty: Option<Type>,
}

impl syn::parse::Parse for ServiceAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut error_ty = None;
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            if ident == "error" {
                let _: syn::Token![=] = input.parse()?;
                error_ty = Some(input.parse()?);
            }
            if input.peek(syn::Token![,]) {
                let _: syn::Token![,] = input.parse()?;
            }
        }
        Ok(Self { error_ty })
    }
}

#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as ServiceAttr);
    let input = parse_macro_input!(item as ItemTrait);
    impl_service(&input, attr.error_ty)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: TokenStream2 = item.into();
    quote! {
        #[rsmp::async_trait]
        #item
    }
    .into()
}

#[proc_macro_attribute]
pub fn local_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: TokenStream2 = item.into();
    quote! {
        #[rsmp::async_trait(?Send)]
        #item
    }
    .into()
}

#[proc_macro_attribute]
pub fn stream_compat(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: TokenStream2 = item.into();
    quote! {
        #[rsmp::async_trait]
        #item
    }
    .into()
}

#[proc_macro_attribute]
pub fn local_stream_compat(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: TokenStream2 = item.into();
    quote! {
        #[rsmp::async_trait(?Send)]
        #item
    }
    .into()
}

fn impl_service(input: &ItemTrait, error_ty: Option<Type>) -> syn::Result<TokenStream2> {
    let trait_name = &input.ident;
    let trait_name_str = trait_name.to_string();
    let mod_name = format_ident!("{}", to_snake_case(&trait_name_str));
    let handler_name = format_ident!("{}Handler", trait_name);
    let local_handler_name = format_ident!("{}HandlerLocal", trait_name);
    let client_name = format_ident!("{}Client", trait_name);

    let mut methods: Vec<MethodInfo> = Vec::new();

    for (idx, item) in input.items.iter().enumerate() {
        let TraitItem::Fn(method) = item else {
            continue;
        };

        let sig = &method.sig;
        let method_name = sig.ident.clone();

        let mut method_idx: Option<u16> = None;
        for attr in &method.attrs {
            if attr.path().is_ident("method") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("idx") {
                        let value: LitInt = meta.value()?.parse()?;
                        method_idx = Some(value.base10_parse()?);
                    }
                    Ok(())
                })?;
            }
        }
        let idx = method_idx.unwrap_or(idx as u16);

        let mut args: Vec<ArgInfo> = Vec::new();
        let mut has_request_stream = false;
        let mut stream_seen = false;

        for arg in sig.inputs.iter().skip(1) {
            if let FnArg::Typed(pat_type) = arg {
                let ty = &*pat_type.ty;
                let name = if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    pat_ident.ident.clone()
                } else {
                    return Err(syn::Error::new_spanned(pat_type, "expected identifier"));
                };

                if is_stream_marker_type(ty) {
                    if stream_seen {
                        return Err(syn::Error::new_spanned(
                            pat_type,
                            "only one Stream argument is allowed",
                        ));
                    }
                    has_request_stream = true;
                    stream_seen = true;
                } else {
                    if stream_seen {
                        return Err(syn::Error::new_spanned(
                            pat_type,
                            "Stream must be the last argument",
                        ));
                    }
                    args.push(ArgInfo {
                        name,
                        ty: ty.clone(),
                        is_option: is_option_type(ty),
                    });
                }
            }
        }

        let return_ty = match &sig.output {
            ReturnType::Type(_, ty) => Some((**ty).clone()),
            ReturnType::Default => None,
        };

        let has_response_stream = return_ty.as_ref().is_some_and(|ty| {
            if let Some(types) = extract_tuple_types(ty) {
                types.iter().any(|t| is_stream_marker_type(t))
            } else {
                is_stream_marker_type(ty)
            }
        });

        let response_ty = return_ty.as_ref().and_then(extract_base_response_type);
        let has_response_data = response_ty.is_some();

        methods.push(MethodInfo {
            name: method_name,
            idx,
            args,
            has_request_stream,
            has_response_stream,
            has_response_data,
            response_ty,
        });
    }

    let mut seen_indices = HashSet::new();
    for m in &methods {
        if !seen_indices.insert(m.idx) {
            return Err(syn::Error::new_spanned(
                input,
                format!("duplicate method index: {}", m.idx),
            ));
        }
    }

    let method_consts = methods.iter().map(|m| {
        let name = format_ident!("{}", m.name.to_string().to_uppercase());
        let idx = m.idx;
        quote! { pub const #name: u16 = #idx; }
    });

    let method_name_arms = methods.iter().map(|m| {
        let idx = m.idx;
        let name_str = m.name.to_string();
        quote! { #idx => Some(#name_str), }
    });

    let error_ty_tokens = error_ty
        .as_ref()
        .map(|t| quote! { #t })
        .unwrap_or_else(|| quote! { std::convert::Infallible });

    let method_name_consts: Vec<_> = methods
        .iter()
        .map(|m| {
            let const_name = format_ident!("{}_NAME", m.name.to_string().to_uppercase());
            let name_str = m.name.to_string();
            quote! { const #const_name: &'static str = #name_str; }
        })
        .collect();

    let handler_methods: Vec<_> = methods
        .iter()
        .map(|m| {
            let name = &m.name;
            let err_ty = &error_ty_tokens;

            let arg_params: Vec<_> = m
                .args
                .iter()
                .map(|a| {
                    let aname = &a.name;
                    let ty = &a.ty;
                    quote! { #aname: #ty }
                })
                .collect();

            match (m.has_request_stream, m.has_response_stream, m.has_response_data) {
                (true, true, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        async fn #name(&self, #(#arg_params,)* stream: &mut C, req_size: u64) -> Result<#resp_ty, #err_ty>;
                    }
                },
                (true, true, false) => {
                    quote! {
                        async fn #name(&self, #(#arg_params,)* stream: &mut C, req_size: u64) -> Result<(), #err_ty>;
                    }
                },
                (true, false, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        async fn #name(&self, #(#arg_params,)* stream: &mut C, size: u64) -> Result<#resp_ty, #err_ty>;
                    }
                },
                (true, false, false) => quote! {
                    async fn #name(&self, #(#arg_params,)* stream: &mut C, size: u64) -> Result<(), #err_ty>;
                },
                (false, true, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        async fn #name(&self, #(#arg_params,)* out: &mut C) -> Result<#resp_ty, #err_ty>;
                    }
                },
                (false, true, false) => {
                    quote! {
                        async fn #name(&self, #(#arg_params,)* out: &mut C) -> Result<(), #err_ty>;
                    }
                },
                (false, false, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        async fn #name(&self, #(#arg_params),*) -> Result<#resp_ty, #err_ty>;
                    }
                },
                (false, false, false) => quote! {
                    async fn #name(&self, #(#arg_params),*) -> Result<(), #err_ty>;
                },
            }
        })
        .collect();

    let dispatch_arms: Vec<_> = methods
        .iter()
        .map(|m| {
            let name = &m.name;
            let idx = m.idx;

            let decode_args: Vec<_> = m
                .args
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    let var_name = format_ident!("__arg_{}", i);
                    let ty = &a.ty;
                    if a.is_option {
                        let inner_ty = extract_option_inner(ty);
                        quote! {
                            let #var_name: #ty = __arg_map.get(&(#i as rsmp::FieldIndex))
                                .map(|(wt, data)| {
                                    if *wt == rsmp::WireType::None {
                                        Ok(None)
                                    } else {
                                        <#inner_ty as rsmp::Decode>::decode(*wt, data).map(Some)
                                    }
                                })
                                .transpose()?
                                .flatten();
                        }
                    } else {
                        quote! {
                            let #var_name: #ty = __arg_map.get(&(#i as rsmp::FieldIndex))
                                .ok_or(rsmp::ProtocolError::MissingField(#i as rsmp::FieldIndex))
                                .and_then(|(wt, data)| <#ty as rsmp::Decode<'_>>::decode(*wt, data))?;
                        }
                    }
                })
                .collect();

            let call_args: Vec<_> = m
                .args
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let var_name = format_ident!("__arg_{}", i);
                    quote! { #var_name }
                })
                .collect();

            match (m.has_request_stream, m.has_response_stream, m.has_response_data) {
                (true, true, true) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args,)* __stream, __req_size).await {
                            Ok(_result) => true,
                            Err(e) => {
                                __stream.write_all(&rsmp::ERROR_MARKER.to_be_bytes()).await?;
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (true, true, false) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args,)* __stream, __req_size).await {
                            Ok(()) => true,
                            Err(e) => {
                                __stream.write_all(&rsmp::ERROR_MARKER.to_be_bytes()).await?;
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (true, false, true) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args,)* __stream, __req_size).await {
                            Ok(result) => {
                                let response = rsmp::Args::encode_args(&result);
                                __stream.write_all(&(response.len() as u32).to_be_bytes()).await?;
                                __stream.write_all(&response).await?;
                                true
                            }
                            Err(e) => {
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32 | 0x8000_0000).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (true, false, false) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args,)* __stream, __req_size).await {
                            Ok(()) => {
                                __stream.write_all(&0u32.to_be_bytes()).await?;
                                true
                            }
                            Err(e) => {
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32 | 0x8000_0000).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (false, true, true) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args,)* __stream).await {
                            Ok(_result) => true,
                            Err(e) => {
                                __stream.write_all(&rsmp::ERROR_MARKER.to_be_bytes()).await?;
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (false, true, false) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args,)* __stream).await {
                            Ok(()) => true,
                            Err(e) => {
                                __stream.write_all(&rsmp::ERROR_MARKER.to_be_bytes()).await?;
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (false, false, true) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args),*).await {
                            Ok(result) => {
                                let response = rsmp::Args::encode_args(&result);
                                __stream.write_all(&(response.len() as u32).to_be_bytes()).await?;
                                __stream.write_all(&response).await?;
                                true
                            }
                            Err(e) => {
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32 | 0x8000_0000).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
                (false, false, false) => quote! {
                    #idx => {
                        #(#decode_args)*
                        match __handler.#name(#(#call_args),*).await {
                            Ok(()) => {
                                __stream.write_all(&0u32.to_be_bytes()).await?;
                                true
                            }
                            Err(e) => {
                                let err_data = rsmp::Args::encode_args(&e);
                                __stream.write_all(&(err_data.len() as u32 | 0x8000_0000).to_be_bytes()).await?;
                                __stream.write_all(&err_data).await?;
                                false
                            }
                        }
                    }
                },
            }
        })
        .collect();

    let client_methods: Vec<_> = methods
        .iter()
        .map(|m| {
            let name = &m.name;
            let idx = m.idx;
            let err_ty = &error_ty_tokens;

            let client_params: Vec<_> = m
                .args
                .iter()
                .map(|a| {
                    let name = &a.name;
                    let ty = &a.ty;
                    quote! { #name: #ty }
                })
                .collect();

            let encode_args: Vec<_> = m
                .args
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    let name = &a.name;
                    if a.is_option {
                        quote! {
                            match &#name {
                                Some(v) => {
                                    let mut __field_data = Vec::new();
                                    rsmp::Encode::encode(v, &mut __field_data);
                                    rsmp::write_field(&mut __buf, #i as rsmp::FieldIndex, rsmp::Encode::wire_type(v), &__field_data);
                                }
                                None => {
                                    rsmp::write_field(&mut __buf, #i as rsmp::FieldIndex, rsmp::WireType::None, &[]);
                                }
                            }
                        }
                    } else {
                        quote! {
                            {
                                let mut __field_data = Vec::new();
                                rsmp::Encode::encode(&#name, &mut __field_data);
                                rsmp::write_field(&mut __buf, #i as rsmp::FieldIndex, rsmp::Encode::wire_type(&#name), &__field_data);
                            }
                        }
                    }
                })
                .collect();

            let field_count = m.args.len();

            let encode_block = quote! {
                let mut __buf = Vec::new();
                __buf.extend_from_slice(&(#field_count as rsmp::FieldIndex).to_be_bytes());
                #(#encode_args)*
            };

            match (m.has_request_stream, m.has_response_stream, m.has_response_data) {
                (true, true, _) => quote! {
                    pub async fn #name(
                        &mut self,
                        #(#client_params,)*
                        body: T::Stream,
                    ) -> Result<T::BoxRead<'_>, rsmp::ClientError<#err_ty>> {
                        #encode_block
                        let (mut __body_reader, __body_size) = rsmp::StreamLike::into_parts(body);
                        match self.transport
                            .call_with_body_and_response_stream_raw(#idx, &__buf, &mut *__body_reader, __body_size)
                            .await?
                        {
                            Ok((_response_data, stream)) => Ok(stream),
                            Err(err_data) => {
                                let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                Err(rsmp::ClientError::Server(err))
                            }
                        }
                    }
                },
                (true, false, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        pub async fn #name(
                            &mut self,
                            #(#client_params,)*
                            body: T::Stream,
                        ) -> Result<#resp_ty, rsmp::ClientError<#err_ty>> {
                            #encode_block
                            let (mut __body_reader, __body_size) = rsmp::StreamLike::into_parts(body);
                            match self.transport
                                .call_with_body_raw(#idx, &__buf, &mut *__body_reader, __body_size)
                                .await?
                            {
                                rsmp::Response::Ok(response_data) => {
                                    Ok(<#resp_ty as rsmp::Args>::decode_args(&response_data)?)
                                }
                                rsmp::Response::Err(err_data) => {
                                    let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                    Err(rsmp::ClientError::Server(err))
                                }
                            }
                        }
                    }
                },
                (true, false, false) => quote! {
                    pub async fn #name(
                        &mut self,
                        #(#client_params,)*
                        body: T::Stream,
                    ) -> Result<(), rsmp::ClientError<#err_ty>> {
                        #encode_block
                        let (mut __body_reader, __body_size) = rsmp::StreamLike::into_parts(body);
                        match self.transport
                            .call_with_body_raw(#idx, &__buf, &mut *__body_reader, __body_size)
                            .await?
                        {
                            rsmp::Response::Ok(_) => Ok(()),
                            rsmp::Response::Err(err_data) => {
                                let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                Err(rsmp::ClientError::Server(err))
                            }
                        }
                    }
                },
                (false, true, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        pub async fn #name(
                            &mut self,
                            #(#client_params,)*
                        ) -> Result<(#resp_ty, T::BoxRead<'_>), rsmp::ClientError<#err_ty>> {
                            #encode_block
                            match self.transport
                                .call_with_response_stream_raw(#idx, &__buf)
                                .await?
                            {
                                Ok(stream) => Ok((<#resp_ty as std::default::Default>::default(), stream)),
                                Err(err_data) => {
                                    let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                    Err(rsmp::ClientError::Server(err))
                                }
                            }
                        }
                    }
                },
                (false, true, false) => quote! {
                    pub async fn #name(
                        &mut self,
                        #(#client_params,)*
                    ) -> Result<T::BoxRead<'_>, rsmp::ClientError<#err_ty>> {
                        #encode_block
                        match self.transport
                            .call_with_response_stream_raw(#idx, &__buf)
                            .await?
                        {
                            Ok(stream) => Ok(stream),
                            Err(err_data) => {
                                let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                Err(rsmp::ClientError::Server(err))
                            }
                        }
                    }
                },
                (false, false, true) => {
                    let resp_ty = m.response_ty.as_ref().unwrap();
                    quote! {
                        pub async fn #name(&mut self, #(#client_params),*) -> Result<#resp_ty, rsmp::ClientError<#err_ty>> {
                            #encode_block
                            match self.transport.call_raw(#idx, &__buf).await? {
                                rsmp::Response::Ok(response_data) => {
                                    Ok(<#resp_ty as rsmp::Args>::decode_args(&response_data)?)
                                }
                                rsmp::Response::Err(err_data) => {
                                    let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                    Err(rsmp::ClientError::Server(err))
                                }
                            }
                        }
                    }
                },
                (false, false, false) => quote! {
                    pub async fn #name(&mut self, #(#client_params),*) -> Result<(), rsmp::ClientError<#err_ty>> {
                        #encode_block
                        match self.transport.call_raw(#idx, &__buf).await? {
                            rsmp::Response::Ok(_) => Ok(()),
                            rsmp::Response::Err(err_data) => {
                                let err = <#err_ty as rsmp::Args>::decode_args(&err_data)?;
                                Err(rsmp::ClientError::Server(err))
                            }
                        }
                    }
                },
            }
        })
        .collect();

    let has_stream_arms = methods.iter().filter(|m| m.has_request_stream).map(|m| {
        let idx = m.idx;
        quote! { #idx => true, }
    });

    let vis = &input.vis;

    let dispatcher_name = format_ident!("{}Dispatcher", trait_name);
    let local_dispatcher_name = format_ident!("{}DispatcherLocal", trait_name);

    Ok(quote! {
        #vis mod #mod_name {
            use super::*;

            #(#method_consts)*

            pub fn has_request_stream(__method_id: u16) -> bool {
                match __method_id {
                    #(#has_stream_arms)*
                    _ => false,
                }
            }

            pub fn method_name(__method_id: u16) -> Option<&'static str> {
                match __method_id {
                    #(#method_name_arms)*
                    _ => None,
                }
            }

            pub fn parse_args(__data: &[u8]) -> Result<std::collections::HashMap<rsmp::FieldIndex, (rsmp::WireType, &[u8])>, rsmp::ProtocolError> {
                const FIELD_INDEX_SIZE: usize = std::mem::size_of::<rsmp::FieldIndex>();
                if __data.len() < FIELD_INDEX_SIZE {
                    return Err(rsmp::ProtocolError::UnexpectedEof);
                }
                let field_count = rsmp::FieldIndex::from_be_bytes(
                    __data[..FIELD_INDEX_SIZE].try_into().unwrap()
                );
                let mut offset = FIELD_INDEX_SIZE;
                let mut map = std::collections::HashMap::new();
                for _ in 0..field_count {
                    let (idx, wire_type, bytes, consumed) = rsmp::read_field(&__data[offset..])?;
                    offset += consumed;
                    map.insert(idx, (wire_type, bytes));
                }
                Ok(map)
            }

            pub async fn do_dispatch<H, C, I>(
                __handler: &H,
                __stream: &mut C,
                __frame: rsmp::RequestFrame,
                __interceptor: &I,
            ) -> Result<(), rsmp::ServiceError>
            where
                H: #handler_name<C>,
                C: rsmp::AsyncStreamCompat,
                I: rsmp::Interceptor,
            {
                let __method_name = method_name(__frame.method_id)
                    .ok_or(rsmp::ServiceError::MethodNotFound(__frame.method_id))?;
                let __call_ctx = rsmp::CallContext {
                    method_id: __frame.method_id as rsmp::FieldIndex,
                    method_name: __method_name,
                };
                let __interceptor_state = __interceptor.before(&__call_ctx);
                let __arg_map = parse_args(&__frame.args_data)?;
                let __req_size = __frame.body_size;
                let __success = match __frame.method_id {
                    #(#dispatch_arms)*
                    _ => return Err(rsmp::ServiceError::MethodNotFound(__frame.method_id)),
                };
                __interceptor.after(&__call_ctx, __interceptor_state, __success);
                Ok(())
            }

            pub async fn do_dispatch_local<H, C, I>(
                __handler: &H,
                __stream: &mut C,
                __frame: rsmp::RequestFrame,
                __interceptor: &I,
            ) -> Result<(), rsmp::ServiceError>
            where
                H: #local_handler_name<C>,
                C: rsmp::AsyncStreamCompat,
                I: rsmp::Interceptor,
            {
                let __method_name = method_name(__frame.method_id)
                    .ok_or(rsmp::ServiceError::MethodNotFound(__frame.method_id))?;
                let __call_ctx = rsmp::CallContext {
                    method_id: __frame.method_id as rsmp::FieldIndex,
                    method_name: __method_name,
                };
                let __interceptor_state = __interceptor.before(&__call_ctx);
                let __arg_map = parse_args(&__frame.args_data)?;
                let __req_size = __frame.body_size;
                let __success = match __frame.method_id {
                    #(#dispatch_arms)*
                    _ => return Err(rsmp::ServiceError::MethodNotFound(__frame.method_id)),
                };
                __interceptor.after(&__call_ctx, __interceptor_state, __success);
                Ok(())
            }

            pub async fn dispatch<H, C>(
                __handler: &H,
                __stream: &mut C,
                __frame: rsmp::RequestFrame,
            ) -> Result<(), rsmp::ServiceError>
            where
                H: #handler_name<C>,
                C: rsmp::AsyncStreamCompat,
            {
                do_dispatch(__handler, __stream, __frame, &()).await
            }

            pub async fn dispatch_local<H, C>(
                __handler: &H,
                __stream: &mut C,
                __frame: rsmp::RequestFrame,
            ) -> Result<(), rsmp::ServiceError>
            where
                H: #local_handler_name<C>,
                C: rsmp::AsyncStreamCompat,
            {
                do_dispatch_local(__handler, __stream, __frame, &()).await
            }
        }

        #vis struct #dispatcher_name<'a, H, C, I = ()> {
            handler: &'a H,
            stream: &'a mut C,
            interceptor: I,
        }

        impl<'a, H, C> #dispatcher_name<'a, H, C, ()> {
            pub fn new(handler: &'a H, stream: &'a mut C) -> Self {
                Self { handler, stream, interceptor: () }
            }
        }

        impl<'a, H, C, I> #dispatcher_name<'a, H, C, I>
        where
            I: rsmp::Interceptor,
        {
            pub fn interceptor<I2: rsmp::Interceptor>(self, interceptor: I2) -> #dispatcher_name<'a, H, C, I2> {
                #dispatcher_name {
                    handler: self.handler,
                    stream: self.stream,
                    interceptor,
                }
            }
        }

        impl<'a, H, C, I> #dispatcher_name<'a, H, C, I>
        where
            H: #handler_name<C>,
            C: rsmp::AsyncStreamCompat,
            I: rsmp::Interceptor,
        {
            pub async fn dispatch(&mut self, frame: rsmp::RequestFrame) -> Result<(), rsmp::ServiceError> {
                #mod_name::do_dispatch(self.handler, self.stream, frame, &self.interceptor).await
            }
        }

        impl<'a, H, C, I> #dispatcher_name<'a, H, C, I>
        where
            H: #local_handler_name<C>,
            C: rsmp::AsyncStreamCompat,
            I: rsmp::Interceptor,
        {
            pub fn local(self) -> #local_dispatcher_name<'a, H, C, I> {
                #local_dispatcher_name {
                    handler: self.handler,
                    stream: self.stream,
                    interceptor: self.interceptor,
                }
            }
        }

        #vis struct #local_dispatcher_name<'a, H, C, I> {
            handler: &'a H,
            stream: &'a mut C,
            interceptor: I,
        }

        impl<'a, H, C, I> #local_dispatcher_name<'a, H, C, I>
        where
            H: #local_handler_name<C>,
            C: rsmp::AsyncStreamCompat,
            I: rsmp::Interceptor,
        {
            pub async fn dispatch(&mut self, frame: rsmp::RequestFrame) -> Result<(), rsmp::ServiceError> {
                #mod_name::do_dispatch_local(self.handler, self.stream, frame, &self.interceptor).await
            }

            pub async fn run(&mut self) -> Result<(), rsmp::ServiceError> {
                loop {
                    let frame = rsmp::RequestFrame::read(self.stream, #mod_name::has_request_stream).await?;
                    #mod_name::do_dispatch_local(self.handler, self.stream, frame, &self.interceptor).await?;
                }
            }

            pub async fn run_until<F: std::future::Future>(mut self, shutdown: F) {
                let mut shutdown = std::pin::pin!(shutdown);
                loop {
                    let frame = {
                        let read_fut = std::pin::pin!(rsmp::RequestFrame::read(self.stream, #mod_name::has_request_stream));
                        match rsmp::futures_util::future::select(&mut shutdown, read_fut).await {
                            rsmp::futures_util::future::Either::Left(_) => return,
                            rsmp::futures_util::future::Either::Right((Ok(frame), _)) => frame,
                            rsmp::futures_util::future::Either::Right((Err(_), _)) => return,
                        }
                    };
                    if #mod_name::do_dispatch_local(self.handler, self.stream, frame, &self.interceptor).await.is_err() {
                        return;
                    }
                }
            }
        }

        #[rsmp::async_trait]
        #vis trait #handler_name<C: rsmp::AsyncStreamCompat>: Send + Sync {
            #(#method_name_consts)*
            #(#handler_methods)*
        }

        #[rsmp::async_trait(?Send)]
        #vis trait #local_handler_name<C: rsmp::AsyncStreamCompat> {
            #(#method_name_consts)*
            #(#handler_methods)*
        }

        #vis struct #client_name<T: rsmp::Transport> {
            transport: T,
        }

        impl<T: rsmp::Transport> #client_name<T> {
            pub fn new(transport: T) -> Self {
                Self { transport }
            }

            #(#client_methods)*
        }

        impl<S: rsmp::futures_util::io::AsyncRead + rsmp::futures_util::io::AsyncWrite + Unpin> #client_name<rsmp::StreamTransport<S>> {
            pub fn from_stream(stream: S) -> Self {
                Self::new(rsmp::StreamTransport::new(stream))
            }
        }
    })
}

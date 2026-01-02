use std::collections::HashSet;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, LitInt, Type, parse_macro_input};

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
    idx: u16,
    ty: Type,
    is_option: bool,
    is_stream: bool,
}

fn impl_args_struct(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "only named fields supported",
                ));
            }
        },
        _ => return Err(syn::Error::new_spanned(input, "only structs supported")),
    };

    let mut field_info: Vec<FieldInfo> = Vec::new();

    for field in fields {
        let field_name = field.ident.clone().unwrap();
        let field_type = field.ty.clone();
        let is_option = is_option_type(&field_type);
        let is_stream = is_stream_type(&field_type);

        let mut field_idx: Option<u16> = None;

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
            is_stream,
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
                            #idx,
                            rsmp::Encode::wire_type(v),
                            &field_data,
                        );
                    }
                    None => {
                        rsmp::write_field(
                            &mut buf,
                            #idx,
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
                        #idx,
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
                            <#inner_ty as rsmp::Decode>::decode(__wire_type, __bytes)?
                        );
                    }
                }
            }
        } else {
            let ty = &f.ty;
            quote! {
                #idx => {
                    #var_name = Some(
                        <#ty as rsmp::Decode>::decode(__wire_type, __bytes)?
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
                #name: #var_name.ok_or(rsmp::ProtocolError::MissingField(#idx))?
            }
        }
    });

    let field_count = field_info.len() as u16;

    let stream_field = field_info.iter().find(|f| f.is_stream);
    let stream_size_impl = stream_field.map(|f| {
        let field_name = &f.name;
        quote! {
            fn stream_size(&self) -> Option<u64> {
                Some(self.#field_name.0)
            }
        }
    });

    Ok(quote! {
        impl rsmp::Args for #name {
            fn encode_args(&self) -> Vec<u8> {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(#field_count as u16).to_be_bytes());
                #(#encode_fields)*
                buf
            }

            fn decode_args(data: &[u8]) -> Result<Self, rsmp::ProtocolError> {
                if data.len() < 2 {
                    return Err(rsmp::ProtocolError::UnexpectedEof);
                }

                let field_count = u16::from_be_bytes([data[0], data[1]]);
                let mut offset = 2usize;

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

            #stream_size_impl
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

fn is_stream_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "Stream";
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
    idx: u16,
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

        let mut variant_idx: Option<u16> = None;

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
                buf.extend_from_slice(&(#idx as u16).to_be_bytes());
                buf.extend_from_slice(&rsmp::Args::encode_args(inner));
            }
        }
    });

    let decode_arms = variant_info.iter().map(|v| {
        let vname = &v.name;
        let idx = v.idx;
        let ty = &v.inner_ty;
        quote! {
            #idx => Ok(Self::#vname(<#ty as rsmp::Args>::decode_args(&data[2..])?)),
        }
    });

    let stream_arms = variant_info.iter().map(|v| {
        let vname = &v.name;
        quote! {
            Self::#vname(inner) => rsmp::Args::stream_size(inner),
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
                if data.len() < 2 {
                    return Err(rsmp::ProtocolError::UnexpectedEof);
                }
                let variant_id = u16::from_be_bytes([data[0], data[1]]);
                match variant_id {
                    #(#decode_arms)*
                    _ => Err(rsmp::ProtocolError::UnknownVariant(variant_id)),
                }
            }

            fn stream_size(&self) -> Option<u64> {
                match self {
                    #(#stream_arms)*
                }
            }
        }
    })
}

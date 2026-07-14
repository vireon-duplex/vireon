//! `#[profile]` attribute macro for the profiler crate.
//!
//! Instruments a function with a timing guard. The function name is
//! captured at compile time as a `&'static str` — no runtime allocation.
//!
//! # Expansion
//!
//! ```ignore
//! #[profile]
//! fn process(data: &[u8]) -> usize {
//!     data.len()
//! }
//! ```
//!
//! becomes:
//!
//! ```ignore
//! fn process(data: &[u8]) -> usize {
//!     let _guard = ::profiler::Guard::enter("process");
//!     data.len()
//! }
//! ```
//!
//! For `async fn`, the guard lives inside the state machine and
//! persists across `.await` points, measuring wall-clock time from
//! first poll to completion.
//!
//! # Supported function kinds
//!
//! - Free functions (`fn foo()`)
//! - Generic functions (`fn foo<T>()`)
//! - Lifetimes (`fn foo<'a>()`)
//! - Async functions (`async fn foo()`)
//! - Unsafe functions (`unsafe fn foo()`)
//! - Methods in `impl` blocks
//! - Associated functions
//! - Trait impl methods
//! - Any visibility (`pub`, `pub(crate)`, private)
//!
//! # Unsupported
//!
//! - `const fn` — the `Drop` impl makes it non-const. The attribute
//!   is silently ignored for `const fn`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, ItemFn};

/// Instruments a function with a timing guard.
///
/// ```ignore
/// use profiler::profile;
///
/// #[profile]
/// fn my_function() {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn profile(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_fn = parse_macro_input!(item as ItemFn);

    match expand(&item_fn) {
        Ok(ts) => ts.into(),
        Err(_) => item_fn.into_token_stream().into(),
    }
}

/// Expands the `#[profile]` attribute on an `ItemFn`.
fn expand(item: &ItemFn) -> syn::Result<TokenStream2> {
    // Skip const fn — Drop makes it non-const.
    if item.sig.constness.is_some() {
        return Ok(item.to_token_stream());
    }

    // Extract the function name as a string literal.
    let name = item.sig.ident.to_string();

    // Build the guard statement.
    let guard_stmt = quote! {
        let _profiler_guard = ::profiler::Guard::enter(#name);
    };

    // Reconstruct the function with the guard prepended to the body.
    let attrs = &item.attrs;
    let vis = &item.vis;
    let sig = &item.sig;
    let block = &item.block;

    // For extern "C" functions with no body (foreign functions), skip.
    if is_foreign_fn(item) {
        return Ok(item.to_token_stream());
    }

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #guard_stmt
            #block
        }
    };

    Ok(expanded)
}

/// Checks whether this is a foreign function declaration (extern block).
///
/// `ItemFn` from a proc-macro attribute is always a function definition,
/// not a foreign declaration. But we check for safety.
fn is_foreign_fn(item: &ItemFn) -> bool {
    item.sig.abi.is_some() && item.block.stmts.is_empty()
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_str;

    /// TokenStream strings have spaces between all tokens.
    /// This helper strips spaces so we can search for compound patterns.
    fn norm(s: &str) -> String {
        s.replace(' ', "")
    }

    #[test]
    fn expand_simple_fn() {
        let input: ItemFn = parse_str("fn foo() -> u32 { 42 }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("Guard::enter"));
        assert!(n.contains("\"foo\""));
    }

    #[test]
    fn expand_async_fn() {
        let input: ItemFn =
            parse_str("async fn send(id: u64) -> Result<(), Error> { Ok(()) }")
                .unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("async"));
        assert!(n.contains("Guard::enter"));
        assert!(n.contains("\"send\""));
    }

    #[test]
    fn expand_generic_fn() {
        let input: ItemFn =
            parse_str("fn identity<T>(x: T) -> T { x }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("Guard::enter"));
        assert!(n.contains("\"identity\""));
    }

    #[test]
    fn expand_unsafe_fn() {
        let input: ItemFn =
            parse_str("unsafe fn raw(x: *const u8) -> u8 { *x }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("unsafe"));
        assert!(n.contains("Guard::enter"));
    }

    #[test]
    fn expand_pub_fn() {
        let input: ItemFn =
            parse_str("pub fn public() -> () { }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("pub"));
    }

    #[test]
    fn expand_pub_crate_fn() {
        let input: ItemFn =
            parse_str("pub(crate) fn internal() { }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("pub(crate)"));
        assert!(n.contains("Guard::enter"));
    }

    #[test]
    fn expand_const_fn_is_noop() {
        let input: ItemFn =
            parse_str("const fn constant() -> u32 { 42 }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        // Should not contain Guard for const fn.
        assert!(!n.contains("Guard::enter"));
        // Should still contain the original function.
        assert!(n.contains("const"));
    }

    #[test]
    fn expand_preserves_attrs() {
        let input: ItemFn =
            parse_str("#[inline] fn with_attr() { }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("inline"));
        assert!(n.contains("Guard::enter"));
    }

    #[test]
    fn expand_method_name() {
        let input: ItemFn = parse_str("fn bar(&self, x: u32) { }").unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("\"bar\""));
    }

    #[test]
    fn expand_lifetime_fn() {
        let input: ItemFn =
            parse_str("fn borrow<'a>(x: &'a [u8]) -> &'a u8 { &x[0] }")
                .unwrap();
        let output = expand(&input).unwrap();
        let n = norm(&output.to_string());
        assert!(n.contains("Guard::enter"));
        assert!(n.contains("\"borrow\""));
    }
}

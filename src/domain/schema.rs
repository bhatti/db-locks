// @generated automatically by Diesel CLI.

diesel::table! {
    lock_items (key) {
        key -> Text,
        version -> Nullable<Text>,
        semaphore_key -> Nullable<Text>,
        data -> Nullable<Text>,
        replace_data -> Nullable<Bool>,
        delete_on_release -> Nullable<Bool>,
        owner -> Nullable<Text>,
        locked -> Nullable<Bool>,
        lease_duration_ms -> BigInt,
        reentrant -> Nullable<Bool>,
        acquire_only_if_already_exists -> Nullable<Bool>,
        refresh_period_ms -> Nullable<BigInt>,
        additional_time_to_wait_for_lock_ms -> Nullable<BigInt>,
        override_time_to_wait_for_lock_ms -> Nullable<BigInt>,
        expires_at -> Nullable<Timestamp>,
        created_by -> Nullable<Text>,
        created_at -> Nullable<Timestamp>,
        updated_by -> Nullable<Text>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    semaphores (key) {
        key -> Text,
        version -> Nullable<Text>,
        data -> Nullable<Text>,
        replace_data -> Nullable<Bool>,
        delete_on_release -> Nullable<Bool>,
        owner -> Nullable<Text>,
        max_size -> Integer,
        lease_duration_ms -> BigInt,
        reentrant -> Nullable<Bool>,
        refresh_period_ms -> Nullable<BigInt>,
        additional_time_to_wait_for_lock_ms -> Nullable<BigInt>,
        created_by -> Nullable<Text>,
        created_at -> Nullable<Timestamp>,
        updated_by -> Nullable<Text>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    lock_items,
    semaphores,
);

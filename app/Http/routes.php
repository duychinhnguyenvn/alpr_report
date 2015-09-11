<?php

/*
|--------------------------------------------------------------------------
| Application Routes
|--------------------------------------------------------------------------
|
| Here is where you can register all of the routes for an application.
| It's a breeze. Simply tell Laravel the URIs it should respond to
| and give it the controller to call when that URI is requested.
|
*/

Route::get('/', function () {
    return view('index',["name" => "chinh"]);
});

Route::post('log', 'LogController@log');
Route::get('log/insert_transform_log_process', 'LogController@fn_refresh_insert_tranform_log_process');
Route::get('log/generate_daily_agg/{start_date?}/{end_date?}', 'LogController@fn_generate_daily_agg');
Route::get('log/generate_monthly_agg', 'LogController@fn_generate_monthly_agg');

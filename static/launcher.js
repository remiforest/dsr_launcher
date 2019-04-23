
var STATUS_TIMER = 5000
var offset = $("#now").data("now")*1000 - new Date().getTime();

$("#new_container_button").click(function(){
    $("#new_container_button").text("Launching new environment");
    $("#new_container_button").attr("disabled", true);
    $("#new_container_loader").show();
    var username = $("#container_username").val();
    var password = $("#container_password").val();
    $.ajax({
        url: 'new_container',
        type: 'post',
        data: {"username":username,"password":password},
        success:function(data){
            window.location.reload();
        }
    });
})


$(".stop_container").click(function(){
    var button = $(this);
    button.html("Stopping <i class='fa fa-cog fa-spin'></i>");
    button.attr("disabled", true);
    $.ajax({
        url: 'stop_container',
        type: 'post',
        data: {"container":$(this).data("container")},
        success:function(data){
            window.location.reload();      
        }
    });
})


$(".delete_container").click(function(){
    var button = $(this);
    button.attr("disabled", true);
    $.ajax({
        url: 'delete_container',
        type: 'post',
        data: {"container":button.data("container")},
        success:function(data){
            window.location.reload();
        }
    });
})

$(".start_container").click(function(){
    var button = $(this);
    button.html("Starting <i class='fa fa-cog fa-spin'></i>");
    button.attr("disabled", true);
    $.ajax({
        url: 'start_container',
        type: 'post',
        data: {"container":button.data("container")},
        success:function(data){
            window.location.reload();
        }
    });
})

function check_status(){
    $(".container_status").each(function(){
        var container = $(this).data("container");
        $.ajax({
            url: 'check_status',
            type: 'post',
            data: {"container":container},
            success:function(data){
                update_status(container,data)
            }
        });
    });
    setTimeout(function(){
        check_status();
    }, STATUS_TIMER);
}

function update_status(container,status){
    $("#"+container+"_id").html(container);
    if(status == "starting"){
        var limit = 30;
        if($("#"+container+"_status").data("start") == $("#"+container+"_status").data("creation")){
            limit = 150;
        } 
        var now = new Date().getTime() + offset;
        var start_time = $("#"+container+"_status").data("start");
        var target = parseInt(start_time) + limit; 
        var distance =  target*1000 - now;
        var minutes = Math.abs(Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60)));
        var seconds = Math.abs(Math.floor((distance % (1000 * 60)) / 1000));
        if(minutes<10){minutes = "0" + minutes};
        if(seconds<10){seconds = "0" + seconds};
        var minus = '';
        if (distance < 0){
            minus = '-';
            minutes = minutes - 1
        }
        $("#"+container+"_status").html(status + '<i class="fa fa-cog fa-spin"></i><span class="start_timer">' + minus + minutes + ':' + seconds + '</span>');
    }else if(status == "error"){
        $("#"+container+"_status").html(status + ' <i class="fa fa-cog fa-exclamation-triangle"></i>');
    }else if(status == "running"){
        $("#"+container+"_id").html('<a href="https://' + window.location.hostname + ':' + $("#"+container+"_id").data("port") + '" target="_blank" data-port="' + $("#"+container+"_id").data("port")+'">'+container+'</a>');
        $("#"+container+"_status").html(status);
    }else{
        $("#"+container+"_status").html(status); 
    }
}



$( document ).ready(function() {
    check_status();
})


$("#reset_button").click(function(){
    $(this).text("Resetting application");
    $(this).attr("disabled", true);
    $.ajax({
        url: 'reset',
        type: 'post',
        success:function(data){
            window.location.reload();      
        }
    });
})

(function(angular) {
    'use strict';

    /** app level module which depends on services and controllers */
    angular.module('playkafka', ['playkafka.controllers', 'ngRoute'])
        .config(['$routeProvider', '$locationProvider',
            function ($routeProvider, $locationProvider) {
                $routeProvider
                    .when('/', {
                        templateUrl: 'assets/html/home.html',
                        controller: 'HomeCtrl',
                        controllerAs: 'home'
                    });

                $locationProvider.html5Mode(true);
            }]);

    angular.module('playkafka.services', []);

})(window.angular);

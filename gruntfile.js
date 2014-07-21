/*global module:false*/
module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({
    // Task configuration.
    jshint: {
      options: {
        esnext: true,
        indent: 2,
        expr: true,
        camelcase: true,
        curly: true,
        eqeqeq: true,
        newcap: true,
        unused: true,
        trailing: true,
        browser: false,
        node: true
      },
      gruntfile: {
        src: 'gruntfile.js'
      },
      tests: {
        src: ['tests/*']
      },
      "index.js": {
        src: ['./index.js']
      },
      lib: {
        src: ['lib/*']
      }
    },
    mochaTest: {
      test: {
        options: {
          reporter: 'spec',
          timeout: 200000
        },
        src: ['tests/*.js']
      }
    }
  });

  // These plugins provide necessary tasks.
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-mocha-test');

  // Default task.
  grunt.registerTask('test', [ 'jshint', 'mochaTest' ]);

};

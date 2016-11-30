#!/bin/bash

#echo "--DEBUG--"
#echo "TRAVIS_REPO_SLUG: $TRAVIS_REPO_SLUG"
#echo "TRAVIS_PHP_VERSION: $TRAVIS_PHP_VERSION"
#echo "TRAVIS_PULL_REQUEST: $TRAVIS_PULL_REQUEST"

if [ "$TRAVIS_REPO_SLUG" == "sportarchive/CloudProcessingEngine-Client-SDK-PHP" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_PHP_VERSION" == "5.6" ]; then

    echo -e "Publishing PHPDoc...\\n"
    ## Copie de la documentation generee dans le $HOME
    cp -R build/docs $HOME/docs-latest

    cd $HOME
    ## Initialisation et recuperation de la branche gh-pages du depot Git
    git config --global user.email "travis@travis-ci.org"
    git config --global user.name "travis-ci"
    git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/sportarchive/CloudProcessingEngine-Client-SDK-PHP gh-pages > /dev/null

    cd gh-pages
    
    ## Suppression de l'ancienne version
    git rm -rf .

    ## Création des dossiers

    ## Copie de la nouvelle version
    cp -Rf $HOME/docs-latest/* .

    ## On ajoute tout
    git add -f .
    ## On commit
    git commit -m "PHPDocumentor (Travis Build : $TRAVIS_BUILD_NUMBER  - Branch : $TRAVIS_BRANCH)"
    ## On push
    git push -fq origin gh-pages > /dev/null
    ## Et c est en ligne !
    echo -e "Published PHPDoc to gh-pages.\\n"

fi
